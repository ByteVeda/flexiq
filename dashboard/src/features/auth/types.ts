export interface AuthUser {
  username: string;
  role: string;
  created_at: number;
  last_login_at: number | null;
}

export interface AuthSession {
  username: string;
  role: string;
  expires_at: number;
  csrf_token: string;
}

export interface LoginResponse {
  user: AuthUser;
  session: AuthSession;
}

export interface SetupResponse {
  user: AuthUser;
}

export interface AuthStatus {
  setup_required: boolean;
  /** Absent on older servers, which always enforce auth. */
  auth_enabled?: boolean;
  /** Public read-only dashboard: every write answers 403. */
  read_only?: boolean;
  /** Same-origin trigger path the read-only banner's enqueue button posts to. */
  demo_enqueue?: string | null;
}

export interface WhoamiResponse {
  user: AuthUser;
  csrf_token: string;
  expires_at: number;
}

/** One entry in the providers listing response. */
export interface AuthProvider {
  /** Stable URL-safe identifier used in the callback path. */
  slot: string;
  /** Human-readable button label. */
  label: string;
  /** Provider type, drives which icon is rendered. */
  type: "google" | "github" | "oidc";
}

export interface ProvidersResponse {
  password_enabled: boolean;
  providers: AuthProvider[];
}
