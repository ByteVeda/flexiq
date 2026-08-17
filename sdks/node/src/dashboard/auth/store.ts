// Authentication primitives for the dashboard.
//
// Users and sessions are persisted through the queue's settings key/value
// store — the same store that backs webhooks and dashboard settings — so the
// feature works uniformly across SQLite, Postgres, and Redis with no new
// tables. The key layout and hash format follow the cross-SDK contract:
//
// - `auth:users` — JSON object `{username: {password_hash, role, ...}}`
// - `auth:session:<token>` — JSON object describing one active session
//
// Password hashes use PBKDF2-HMAC-SHA256 with 600,000 iterations (OWASP
// 2023+ baseline), encoded as `pbkdf2_sha256$<iters>$<salt_hex>$<hash_hex>`.

import { pbkdf2 as pbkdf2Cb, randomBytes, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";
import type { Queue } from "../../index";
import { updateSetting } from "../../settingsKv";
import { createLogger } from "../../utils";
import { ValidationError } from "../errors";

const log = createLogger("dashboard");
const pbkdf2 = promisify(pbkdf2Cb);

// ── Storage keys ────────────────────────────────────────────────────────

export const USERS_KEY = "auth:users";
export const SESSION_PREFIX = "auth:session:";

// ── Crypto parameters ───────────────────────────────────────────────────

export const PBKDF2_ITERATIONS = 600_000;
const PBKDF2_SALT_BYTES = 16;
const PBKDF2_HASH_BYTES = 32;
const SESSION_TOKEN_BYTES = 32;

/** Session lifetime: 24 hours (seconds). */
export const DEFAULT_SESSION_TTL_SECONDS = 24 * 60 * 60;

// ── Validation ──────────────────────────────────────────────────────────

const USERNAME_MAX_LEN = 64;
const PASSWORD_MIN_LEN = 8;
const PASSWORD_MAX_LEN = 256;
/** A dashboard user's access level; the wire form is stored in settings. */
export type Role = "admin" | "viewer";

/** Every role, for runtime validation. */
export const ROLES: readonly Role[] = ["admin", "viewer"];
const USERNAME_RE = /^[\p{L}\p{N}._-]+$/u;

/** Sentinel `password_hash` prefix for OAuth-only users — password login always fails. */
export const OAUTH_PASSWORD_HASH_PREFIX = "oauth:";

// Fixed hash used to keep authentication timing constant for unknown users.
const DUMMY_HASH =
  "pbkdf2_sha256$600000$" +
  "00000000000000000000000000000000$" +
  "0000000000000000000000000000000000000000000000000000000000000000";

// ── Password hashing ────────────────────────────────────────────────────

/** Hash a password with PBKDF2-HMAC-SHA256 into a self-describing string. */
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(PBKDF2_SALT_BYTES);
  const digest = await pbkdf2(password, salt, PBKDF2_ITERATIONS, PBKDF2_HASH_BYTES, "sha256");
  return `pbkdf2_sha256$${PBKDF2_ITERATIONS}$${salt.toString("hex")}$${digest.toString("hex")}`;
}

/** Constant-time verify of a password against an encoded hash. */
export async function verifyPassword(password: string, encoded: string): Promise<boolean> {
  if (encoded.startsWith(OAUTH_PASSWORD_HASH_PREFIX)) {
    return false;
  }
  const parts = encoded.split("$");
  if (parts.length !== 4 || parts[0] !== "pbkdf2_sha256") {
    return false;
  }
  const iterations = Number(parts[1]);
  if (!Number.isInteger(iterations) || iterations <= 0) {
    return false;
  }
  let salt: Buffer;
  let expected: Buffer;
  try {
    salt = Buffer.from(parts[2] ?? "", "hex");
    expected = Buffer.from(parts[3] ?? "", "hex");
  } catch {
    return false;
  }
  if (expected.length === 0) {
    return false;
  }
  const candidate = await pbkdf2(password, salt, iterations, expected.length, "sha256");
  return candidate.length === expected.length && timingSafeEqual(candidate, expected);
}

/** Cryptographically secure URL-safe session token. */
export function generateSessionToken(): string {
  return randomBytes(SESSION_TOKEN_BYTES).toString("base64url");
}

// ── Types ───────────────────────────────────────────────────────────────

/** A persisted dashboard user. */
export interface DashboardUser {
  username: string;
  passwordHash: string;
  role: Role;
  createdAt: number;
  lastLoginAt: number | null;
  email: string | null;
  displayName: string | null;
}

/** An active dashboard session. */
export interface DashboardSession {
  token: string;
  username: string;
  role: Role;
  createdAt: number;
  expiresAt: number;
  csrfToken: string;
}

export const isOauthUser = (user: DashboardUser): boolean =>
  user.passwordHash.startsWith(OAUTH_PASSWORD_HASH_PREFIX);

export const sessionExpired = (session: DashboardSession, now = nowSeconds()): boolean =>
  now >= session.expiresAt;

const nowSeconds = (): number => Math.floor(Date.now() / 1000);

// ── Validation helpers ──────────────────────────────────────────────────

function validateUsername(username: string): void {
  if (!username) {
    throw new ValidationError("username must not be empty");
  }
  if (username.length > USERNAME_MAX_LEN) {
    throw new ValidationError(`username must be <= ${USERNAME_MAX_LEN} chars`);
  }
  if (!USERNAME_RE.test(username)) {
    throw new ValidationError("username may only contain letters, digits, '.', '_', or '-'");
  }
}

function validatePassword(password: string): void {
  if (password.length < PASSWORD_MIN_LEN) {
    throw new ValidationError(`password must be >= ${PASSWORD_MIN_LEN} chars`);
  }
  if (password.length > PASSWORD_MAX_LEN) {
    throw new ValidationError(`password must be <= ${PASSWORD_MAX_LEN} chars`);
  }
}

function validateRole(role: string): asserts role is Role {
  if (!(ROLES as readonly string[]).includes(role)) {
    throw new ValidationError(`role must be one of ${[...ROLES].sort().join(", ")}`);
  }
}

/** Read a persisted role, failing closed on anything unrecognized. */
function roleOrViewer(stored: unknown): Role {
  return (ROLES as readonly unknown[]).includes(stored) ? (stored as Role) : "viewer";
}

/**
 * Role for a freshly-created OAuth user. `admin` requires a verified email
 * AND membership in the admin list — everyone else, including the very first
 * user, gets `viewer`, so a stray first OAuth login can never win admin.
 */
export function oauthBootstrapRole(options: {
  email: string | null;
  emailVerified: boolean;
  adminEmails: readonly string[];
}): Role {
  if (!options.emailVerified || !options.email) {
    return "viewer";
  }
  const normalised = options.email.toLowerCase();
  return options.adminEmails.some((e) => e.toLowerCase() === normalised) ? "admin" : "viewer";
}

// ── Persisted row shapes (cross-SDK snake_case JSON) ────────────────────

interface UserRow {
  password_hash: string;
  role: string;
  created_at: number;
  last_login_at: number | null;
  email?: string | null;
  display_name?: string | null;
}

interface SessionRow {
  username: string;
  role: string;
  created_at: number;
  expires_at: number;
  csrf_token: string;
}

// ── Auth store ──────────────────────────────────────────────────────────

/** Read/write users and sessions through the queue's settings store. */
export class AuthStore {
  constructor(private readonly queue: Queue) {}

  // ── Users ─────────────────────────────────────────────────────────

  private static decodeUsers(raw: string | null): Record<string, UserRow> {
    if (!raw) {
      return {};
    }
    try {
      const data = JSON.parse(raw);
      return data && typeof data === "object" && !Array.isArray(data) ? data : {};
    } catch {
      log.warn(() => "auth:users entry is not valid JSON; treating as empty");
      return {};
    }
  }

  private loadUsers(): Record<string, UserRow> {
    return AuthStore.decodeUsers(this.queue.getSetting(USERS_KEY));
  }

  /**
   * Apply `mutate` to the user table without losing a concurrent edit.
   *
   * All of them live in one JSON document, so an unconditional write would drop
   * a user another dashboard replica had just created.
   */
  private updateUsers<Outcome>(mutate: (users: Record<string, UserRow>) => Outcome): Outcome {
    return updateSetting(this.queue, USERS_KEY, AuthStore.decodeUsers, mutate);
  }

  countUsers(): number {
    return Object.keys(this.loadUsers()).length;
  }

  listUsers(): DashboardUser[] {
    return Object.entries(this.loadUsers()).map(([name, row]) => rowToUser(name, row));
  }

  getUser(username: string): DashboardUser | undefined {
    const row = this.loadUsers()[username];
    return row ? rowToUser(username, row) : undefined;
  }

  async createUser(
    username: string,
    password: string,
    role: Role | string = "admin",
  ): Promise<DashboardUser> {
    validateUsername(username);
    validatePassword(password);
    validateRole(role);
    if (this.loadUsers()[username]) {
      throw new ValidationError(`user '${username}' already exists`);
    }
    // Hashed outside the conditional write: PBKDF2 is the expensive part and a
    // retry must not redo it (nor produce a different salt each attempt).
    const row: UserRow = {
      password_hash: await hashPassword(password),
      role,
      created_at: Date.now(),
      last_login_at: null,
    };
    this.updateUsers((users) => {
      if (users[username]) {
        throw new ValidationError(`user '${username}' already exists`);
      }
      users[username] = row;
    });
    return rowToUser(username, row);
  }

  async updatePassword(username: string, newPassword: string): Promise<void> {
    validatePassword(newPassword);
    const passwordHash = await hashPassword(newPassword);
    this.updateUsers((users) => {
      const row = users[username];
      if (!row) {
        throw new ValidationError(`user '${username}' does not exist`);
      }
      row.password_hash = passwordHash;
    });
  }

  deleteUser(username: string): boolean {
    const removed = this.updateUsers((users) => {
      if (!users[username]) {
        return false;
      }
      delete users[username];
      return true;
    });
    if (!removed) {
      return false;
    }
    // Revoke live sessions so a deleted account stops authorizing now,
    // not at session expiry.
    this.deleteSessionsForUser(username);
    return true;
  }

  private deleteSessionsForUser(username: string): void {
    for (const [key, value] of Object.entries(this.queue.listSettings())) {
      if (!key.startsWith(SESSION_PREFIX)) {
        continue;
      }
      try {
        if (JSON.parse(value)?.username === username) {
          this.queue.deleteSetting(key);
        }
      } catch {
        // Malformed rows are ignored here; pruning handles them.
      }
    }
  }

  /** The user iff username+password match; updates `last_login_at`. */
  async authenticate(username: string, password: string): Promise<DashboardUser | undefined> {
    const users = this.loadUsers();
    const row = users[username];
    if (!row) {
      // Dummy verify keeps timing constant for unknown vs. known usernames.
      await verifyPassword(password, DUMMY_HASH);
      return undefined;
    }
    if (!(await verifyPassword(password, row.password_hash))) {
      return undefined;
    }
    // Stamped conditionally, and only on the row as it stands now: a user
    // deleted between the read and the stamp stays deleted rather than being
    // resurrected by writing the whole document back.
    const stampedAt = Date.now();
    this.updateUsers((current) => {
      const live = current[username];
      if (live) {
        live.last_login_at = stampedAt;
      }
    });
    row.last_login_at = stampedAt;
    return rowToUser(username, row);
  }

  // ── OAuth users ───────────────────────────────────────────────────

  /**
   * Look up or create the user row backing an OAuth identity. Username is
   * `<slot>:<subject>`. On first sight the role comes from
   * {@link oauthBootstrapRole}; later logins refresh email/display name only.
   */
  getOrCreateOauthUser(options: {
    slot: string;
    subject: string;
    email: string | null;
    name: string | null;
    emailVerified: boolean;
    adminEmails?: readonly string[];
  }): DashboardUser {
    const username = `${options.slot}:${options.subject}`;
    const now = Date.now();
    const role = oauthBootstrapRole({
      email: options.email,
      emailVerified: options.emailVerified,
      adminEmails: options.adminEmails ?? [],
    });
    const row = this.updateUsers((users) => {
      const existing = users[username];
      if (existing) {
        if (options.email && existing.email !== options.email) {
          existing.email = options.email;
        }
        if (options.name && existing.display_name !== options.name) {
          existing.display_name = options.name;
        }
        existing.last_login_at = now;
        return existing;
      }
      // `role` is only consulted here, on first sight: a later login must not
      // re-derive it and undo an administrator's change.
      users[username] = {
        password_hash: `${OAUTH_PASSWORD_HASH_PREFIX}${options.slot}`,
        role,
        created_at: now,
        last_login_at: now,
        email: options.email,
        display_name: options.name,
      };
      return users[username];
    });
    return rowToUser(username, row);
  }

  // ── Sessions ──────────────────────────────────────────────────────

  createSession(user: DashboardUser, ttlSeconds = DEFAULT_SESSION_TTL_SECONDS): DashboardSession {
    const now = nowSeconds();
    const session: DashboardSession = {
      token: generateSessionToken(),
      username: user.username,
      role: user.role,
      createdAt: now,
      expiresAt: now + ttlSeconds,
      csrfToken: generateSessionToken(),
    };
    const row: SessionRow = {
      username: session.username,
      role: session.role,
      created_at: session.createdAt,
      expires_at: session.expiresAt,
      csrf_token: session.csrfToken,
    };
    this.queue.setSetting(SESSION_PREFIX + session.token, JSON.stringify(row));
    return session;
  }

  getSession(token: string): DashboardSession | undefined {
    if (!token) {
      return undefined;
    }
    const raw = this.queue.getSetting(SESSION_PREFIX + token);
    if (!raw) {
      return undefined;
    }
    let row: SessionRow;
    try {
      row = JSON.parse(raw);
    } catch {
      return undefined;
    }
    if (
      typeof row?.username !== "string" ||
      typeof row.role !== "string" ||
      typeof row.expires_at !== "number" ||
      typeof row.csrf_token !== "string"
    ) {
      return undefined;
    }
    const session: DashboardSession = {
      token,
      username: row.username,
      role: roleOrViewer(row.role),
      createdAt: row.created_at ?? 0,
      expiresAt: row.expires_at,
      csrfToken: row.csrf_token,
    };
    if (sessionExpired(session)) {
      this.deleteSession(token);
      return undefined;
    }
    return session;
  }

  deleteSession(token: string): boolean {
    return token ? this.queue.deleteSetting(SESSION_PREFIX + token) : false;
  }

  /** Best-effort cleanup of expired session entries. Returns count removed. */
  pruneExpiredSessions(): number {
    const now = nowSeconds();
    let removed = 0;
    for (const [key, value] of Object.entries(this.queue.listSettings())) {
      if (!key.startsWith(SESSION_PREFIX)) {
        continue;
      }
      let expiresAt: number;
      try {
        expiresAt = Number(JSON.parse(value)?.expires_at ?? 0);
      } catch {
        continue;
      }
      if (!Number.isFinite(expiresAt) || expiresAt <= now) {
        this.queue.deleteSetting(key);
        removed += 1;
      }
    }
    return removed;
  }
}

function rowToUser(username: string, row: UserRow): DashboardUser {
  return {
    username,
    passwordHash: row.password_hash,
    role: roleOrViewer(row.role),
    createdAt: Number(row.created_at) || 0,
    lastLoginAt:
      row.last_login_at === null || row.last_login_at === undefined
        ? null
        : Number(row.last_login_at),
    email: typeof row.email === "string" && row.email ? row.email : null,
    displayName: typeof row.display_name === "string" && row.display_name ? row.display_name : null,
  };
}

// ── Bootstrap from environment ──────────────────────────────────────────

/**
 * Idempotently create the first admin from `FLEXIQ_DASHBOARD_ADMIN_USER` /
 * `FLEXIQ_DASHBOARD_ADMIN_PASSWORD`. The password is removed from the
 * process environment immediately after it is read so it cannot later be
 * harvested from `/proc/<pid>/environ` or a crash reporter.
 */
export async function bootstrapAdminFromEnv(queue: Queue): Promise<DashboardUser | undefined> {
  const username = process.env.FLEXIQ_DASHBOARD_ADMIN_USER;
  const password = process.env.FLEXIQ_DASHBOARD_ADMIN_PASSWORD;
  delete process.env.FLEXIQ_DASHBOARD_ADMIN_PASSWORD;
  if (!username || !password) {
    return undefined;
  }
  const store = new AuthStore(queue);
  if (store.getUser(username)) {
    return undefined;
  }
  try {
    const user = await store.createUser(username, password, "admin");
    log.info(() => `bootstrapped admin user '${username}' from environment`);
    return user;
  } catch (error) {
    log.warn(() => `failed to bootstrap admin '${username}' from env: ${String(error)}`);
    return undefined;
  }
}
