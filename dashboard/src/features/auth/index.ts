export { AuthGate } from "./components/auth-gate";
export { LoginForm } from "./components/login-form";
export { OAuthButton } from "./components/oauth-button";
export { SetupForm } from "./components/setup-form";
export { UserMenu } from "./components/user-menu";
export {
  authStatusQuery,
  providersQuery,
  useAuthProviders,
  useAuthStatus,
  useCanOperate,
  useChangePassword,
  useLogin,
  useLogout,
  useSetup,
  useWhoami,
  whoamiQuery,
} from "./hooks";
export type {
  AuthProvider,
  AuthSession,
  AuthStatus,
  AuthUser,
  LoginResponse,
  ProvidersResponse,
  SetupResponse,
  WhoamiResponse,
} from "./types";
