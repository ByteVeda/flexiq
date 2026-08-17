import { generateKeyPairSync, sign as signRaw } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeEach, describe, expect, it } from "vitest";
import {
  AllowlistDenied,
  AuthStore,
  callbackUrl,
  IdentityFetchError,
  OAuthConfigError,
  OAuthFlow,
  type OAuthProvider,
  OAuthStateStore,
  oauthConfigFromEnv,
  type ProviderIdentity,
  StateValidationError,
  s256Challenge,
  validateIdToken,
} from "../../src/dashboard/auth";
import { isSafeRedirect } from "../../src/dashboard/urlSafety";
import { Queue } from "../../src/index";

let queue: Queue;

beforeEach(() => {
  const db = join(mkdtempSync(join(tmpdir(), "taskito-oauth-")), "q.db");
  queue = new Queue({ dbPath: db });
});

const BASE_ENV = {
  FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL: "https://ops.example.com",
};

describe("oauthConfigFromEnv", () => {
  it("returns undefined when nothing is configured", () => {
    expect(oauthConfigFromEnv({})).toBeUndefined();
  });

  it("fails fast on a provider without a base url", () => {
    expect(() => oauthConfigFromEnv({ FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID: "cid" })).toThrow(
      OAuthConfigError,
    );
  });

  it("fails fast on a client id without a secret", () => {
    expect(() =>
      oauthConfigFromEnv({ ...BASE_ENV, FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID: "cid" }),
    ).toThrow(/CLIENT_SECRET/);
  });

  it("parses google, github, and multiple oidc slots", () => {
    const config = oauthConfigFromEnv({
      ...BASE_ENV,
      FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID: "gid",
      FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET: "gsec",
      FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS: "corp.com, example.org",
      FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID: "hid",
      FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET: "hsec",
      FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS: "byteveda",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS: "okta,keycloak",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_ID: "oid",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_SECRET: "osec",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_DISCOVERY_URL: "https://okta/.well-known/x",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_KEYCLOAK_CLIENT_ID: "kid",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_KEYCLOAK_CLIENT_SECRET: "ksec",
      FLEXIQ_DASHBOARD_OAUTH_OIDC_KEYCLOAK_DISCOVERY_URL: "https://kc/.well-known/x",
      FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS: "boss@corp.com",
    });
    expect(config?.google?.allowedDomains).toEqual(["corp.com", "example.org"]);
    expect(config?.github?.allowedOrgs).toEqual(["byteveda"]);
    expect(config?.oidc.map((o) => o.slot)).toEqual(["okta", "keycloak"]);
    expect(config?.adminEmails).toEqual(["boss@corp.com"]);
    expect(callbackUrl(config ?? ({} as never), "okta")).toBe(
      "https://ops.example.com/api/auth/oauth/callback/okta",
    );
  });

  it("rejects http base urls for non-local hosts but allows localhost", () => {
    expect(() =>
      oauthConfigFromEnv({
        FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL: "http://ops.example.com",
        FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID: "cid",
        FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET: "sec",
      }),
    ).toThrow(/https/);
    expect(
      oauthConfigFromEnv({
        FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL: "http://localhost:8787",
        FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID: "cid",
        FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET: "sec",
      }),
    ).toBeDefined();
  });

  it("rejects reserved and duplicate oidc slots", () => {
    expect(() =>
      oauthConfigFromEnv({
        ...BASE_ENV,
        FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS: "google",
        FLEXIQ_DASHBOARD_OAUTH_OIDC_GOOGLE_CLIENT_ID: "x",
        FLEXIQ_DASHBOARD_OAUTH_OIDC_GOOGLE_CLIENT_SECRET: "y",
        FLEXIQ_DASHBOARD_OAUTH_OIDC_GOOGLE_DISCOVERY_URL: "https://z",
      }),
    ).toThrow(/collides/);
    expect(() =>
      oauthConfigFromEnv({
        ...BASE_ENV,
        FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS: "okta,okta",
        FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_ID: "oid",
        FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_SECRET: "osec",
        FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_DISCOVERY_URL: "https://okta/.well-known/x",
      }),
    ).toThrow(/twice/);
  });

  it("rejects disabling passwords with no providers", () => {
    expect(() =>
      oauthConfigFromEnv({
        ...BASE_ENV,
        FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED: "false",
      }),
    ).toThrow(/no way to log in/);
  });
});

describe("state store", () => {
  it("consume is single-use and rejects unknown or expired states", () => {
    const store = new OAuthStateStore(queue);
    const state = store.create("google", "/jobs");
    const consumed = store.consume(state.state);
    expect(consumed?.slot).toBe("google");
    expect(consumed?.nextUrl).toBe("/jobs");
    expect(consumed?.codeVerifier.length).toBeGreaterThanOrEqual(43);
    // Replay: gone.
    expect(store.consume(state.state)).toBeUndefined();
    expect(store.consume("unknown")).toBeUndefined();

    const expired = store.create("google", "/", -1);
    expect(store.consume(expired.state)).toBeUndefined();
  });

  it("prunes expired rows only", () => {
    const store = new OAuthStateStore(queue);
    store.create("google", "/", -1);
    const live = store.create("google", "/");
    expect(store.pruneExpired()).toBe(1);
    expect(store.consume(live.state)).toBeDefined();
  });
});

describe("id_token validation", () => {
  const { publicKey, privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const jwk = publicKey.export({ format: "jwk" }) as Record<string, unknown>;
  const jwks = { keys: [{ ...jwk, kid: "k1" }] };

  const mint = (claims: Record<string, unknown>, header: Record<string, unknown> = {}) => {
    const h = Buffer.from(JSON.stringify({ alg: "RS256", kid: "k1", ...header })).toString(
      "base64url",
    );
    const p = Buffer.from(JSON.stringify(claims)).toString("base64url");
    const sig = signRaw("sha256", Buffer.from(`${h}.${p}`), privateKey).toString("base64url");
    return `${h}.${p}.${sig}`;
  };

  const baseClaims = {
    iss: "https://issuer.example",
    aud: "client-1",
    sub: "user-1",
    nonce: "n-1",
    exp: Math.floor(Date.now() / 1000) + 300,
    email: "dev@corp.com",
    email_verified: true,
  };

  const validate = (token: string, overrides: Record<string, unknown> = {}) =>
    validateIdToken({
      idToken: token,
      jwks,
      issuer: "https://issuer.example",
      clientId: "client-1",
      expectedNonce: "n-1",
      ...overrides,
    });

  it("accepts a valid token and returns claims", () => {
    const claims = validate(mint(baseClaims));
    expect(claims.sub).toBe("user-1");
    expect(claims.email).toBe("dev@corp.com");
  });

  it("rejects a tampered signature", () => {
    const token = mint(baseClaims);
    const tampered = `${token.slice(0, -4)}AAAA`;
    expect(() => validate(tampered)).toThrow(IdentityFetchError);
  });

  it("rejects issuer, audience, and nonce mismatches", () => {
    expect(() => validate(mint({ ...baseClaims, iss: "https://evil" }))).toThrow(/issuer/);
    expect(() => validate(mint({ ...baseClaims, aud: "other" }))).toThrow(/audience/);
    expect(() => validate(mint({ ...baseClaims, nonce: "wrong" }))).toThrow(/nonce/);
  });

  it("rejects expiry beyond the 60s skew and missing sub", () => {
    const past = Math.floor(Date.now() / 1000) - 120;
    expect(() => validate(mint({ ...baseClaims, exp: past }))).toThrow(/expired/);
    expect(() => validate(mint({ ...baseClaims, sub: undefined }))).toThrow(/sub/);
  });

  it("rejects unsupported algorithms outright", () => {
    expect(() => validate(mint(baseClaims, { alg: "HS256" }))).toThrow(/unsupported/);
  });
});

describe("flow", () => {
  class FakeProvider implements OAuthProvider {
    readonly slot = "fake";
    readonly label = "Fake";
    readonly type = "oidc";
    lastAuth?: { state: string; nonce: string; codeChallenge: string; redirectUri: string };
    identity: ProviderIdentity = {
      slot: "fake",
      subject: "sub-1",
      email: "dev@corp.com",
      emailVerified: true,
      name: "Dev",
      picture: null,
    };
    deny = false;

    authorizationUrl(params: {
      state: string;
      nonce: string;
      codeChallenge: string;
      redirectUri: string;
    }): Promise<string> {
      this.lastAuth = params;
      return Promise.resolve(`https://provider.example/authorize?state=${params.state}`);
    }

    exchangeCode(): Promise<ProviderIdentity> {
      return Promise.resolve(this.identity);
    }

    checkAllowlist(): void {
      if (this.deny) {
        throw new AllowlistDenied("nope");
      }
    }
  }

  const makeFlow = (provider: FakeProvider) =>
    new OAuthFlow(
      queue,
      {
        redirectBaseUrl: "https://ops.example.com",
        oidc: [],
        passwordAuthEnabled: true,
        adminEmails: [],
      },
      { providers: new Map([["fake", provider]]) },
    );

  it("start mints state with a PKCE challenge and sanitises next", async () => {
    const provider = new FakeProvider();
    const flow = makeFlow(provider);
    const url = await flow.start("fake", "https://evil.example/phish");
    expect(url).toContain("https://provider.example/authorize");
    expect(provider.lastAuth?.redirectUri).toBe(
      "https://ops.example.com/api/auth/oauth/callback/fake",
    );

    const consumed = new OAuthStateStore(queue).consume(provider.lastAuth?.state ?? "");
    expect(consumed?.nextUrl).toBe("/"); // unsafe next fell back
    expect(provider.lastAuth?.codeChallenge).toBe(s256Challenge(consumed?.codeVerifier ?? ""));
  });

  it("callback creates the user + session and enforces slot match", async () => {
    const provider = new FakeProvider();
    const flow = makeFlow(provider);
    await flow.start("fake", "/jobs");
    const state = provider.lastAuth?.state ?? "";

    const { session, nextUrl } = await flow.handleCallback("fake", {
      code: "code-1",
      stateToken: state,
      error: null,
    });
    expect(nextUrl).toBe("/jobs");
    expect(session.username).toBe("fake:sub-1");
    // Admin comes only from the allowlist — first OAuth user is a viewer.
    expect(session.role).toBe("viewer");
    expect(new AuthStore(queue).getUser("fake:sub-1")?.email).toBe("dev@corp.com");

    // Replayed state fails.
    await expect(
      flow.handleCallback("fake", { code: "code-1", stateToken: state, error: null }),
    ).rejects.toThrow(StateValidationError);
  });

  it("rejects provider errors, allowlist denials, and slot mismatches", async () => {
    const provider = new FakeProvider();
    const flow = makeFlow(provider);

    await expect(
      flow.handleCallback("fake", { code: null, stateToken: null, error: "access_denied" }),
    ).rejects.toThrow(IdentityFetchError);

    await flow.start("fake", "/");
    const state = provider.lastAuth?.state ?? "";
    await expect(
      flow.handleCallback("other", { code: "c", stateToken: state, error: null }),
    ).rejects.toThrow(StateValidationError);

    provider.deny = true;
    await flow.start("fake", "/");
    await expect(
      flow.handleCallback("fake", {
        code: "c",
        stateToken: provider.lastAuth?.state ?? "",
        error: null,
      }),
    ).rejects.toThrow(AllowlistDenied);
  });

  it("applies the admin-emails allowlist over first-user-wins", async () => {
    const provider = new FakeProvider();
    provider.identity = { ...provider.identity, email: "viewer@corp.com" };
    const flow = new OAuthFlow(
      queue,
      {
        redirectBaseUrl: "https://ops.example.com",
        oidc: [],
        passwordAuthEnabled: true,
        adminEmails: ["boss@corp.com"],
      },
      { providers: new Map([["fake", provider]]) },
    );
    await flow.start("fake", "/");
    const { session } = await flow.handleCallback("fake", {
      code: "c",
      stateToken: provider.lastAuth?.state ?? "",
      error: null,
    });
    expect(session.role).toBe("viewer"); // not on the admin list
  });
});

describe("isSafeRedirect", () => {
  it("accepts rooted relative paths and rejects everything else", () => {
    expect(isSafeRedirect("/jobs?tab=1")).toBe(true);
    expect(isSafeRedirect("/")).toBe(true);
    expect(isSafeRedirect("")).toBe(false);
    expect(isSafeRedirect("jobs")).toBe(false);
    expect(isSafeRedirect("//evil.com/x")).toBe(false);
    expect(isSafeRedirect("/\\evil.com")).toBe(false);
    expect(isSafeRedirect("https://evil.com/x")).toBe(false);
  });
});
