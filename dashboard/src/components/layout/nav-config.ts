import {
  Activity,
  BarChart3,
  Box,
  CircuitBoard,
  Cog,
  GitBranch,
  KeyRound,
  LayoutDashboard,
  ListTree,
  type LucideIcon,
  Plug,
  Radio,
  ScrollText,
  Server,
  Settings2,
  Skull,
  Webhook as WebhookIcon,
} from "lucide-react";

export interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  /**
   * Route only some servers serve. Hidden until the server confirms it: the
   * standalone scheduler exposes executors and gRPC tokens, an SDK dashboard
   * does not.
   */
  optional?: boolean;
}

export interface NavGroup {
  title: string;
  items: NavItem[];
}

/**
 * Single source of truth for the primary navigation. Consumed by both the
 * desktop {@link Sidebar} and the {@link MobileMenu} so the two never drift.
 */
export const NAV: NavGroup[] = [
  {
    title: "Monitoring",
    items: [
      { to: "/", label: "Overview", icon: LayoutDashboard },
      { to: "/jobs", label: "Jobs", icon: ListTree },
      { to: "/metrics", label: "Metrics", icon: BarChart3 },
      { to: "/logs", label: "Logs", icon: ScrollText },
      { to: "/workflows", label: "Workflows", icon: GitBranch },
      { to: "/topics", label: "Topics", icon: Radio },
    ],
  },
  {
    title: "Infrastructure",
    items: [
      { to: "/queues", label: "Queues", icon: Box },
      { to: "/workers", label: "Workers", icon: Server },
      { to: "/executors", label: "Executors", icon: Plug, optional: true },
      { to: "/resources", label: "Resources", icon: Activity },
    ],
  },
  {
    title: "Reliability",
    items: [
      { to: "/dead-letters", label: "Dead letters", icon: Skull },
      { to: "/circuit-breakers", label: "Circuit breakers", icon: CircuitBoard },
      { to: "/system", label: "System", icon: Settings2 },
    ],
  },
  {
    title: "Configuration",
    items: [
      { to: "/tasks", label: "Tasks", icon: ListTree },
      { to: "/webhooks", label: "Webhooks", icon: WebhookIcon },
      { to: "/grpc-tokens", label: "gRPC tokens", icon: KeyRound, optional: true },
      { to: "/settings", label: "Settings", icon: Cog },
    ],
  },
];

/**
 * `NAV` with unsupported routes removed, and any group they emptied dropped
 * with them.
 *
 * `supported` is keyed by path so a future optional route needs no new
 * plumbing; `undefined` means "not known yet", which reads as hidden — better
 * a nav entry that appears a moment late than one that vanishes under the
 * cursor.
 */
export function visibleNav(supported: Record<string, boolean | undefined>): NavGroup[] {
  return NAV.map((group) => ({
    ...group,
    items: group.items.filter((item) => !item.optional || supported[item.to] === true),
  })).filter((group) => group.items.length > 0);
}
