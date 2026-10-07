import { Link } from "@tanstack/react-router";

const LINK = "font-mono text-[0.82rem] text-accent hover:underline";
const PLAIN = "font-mono text-[0.82rem] text-[var(--fg-muted)]";

interface Props {
  kind: string;
  target: string;
}

/**
 * A record's target, linked to the dashboard page that shows it. Kinds with no
 * page of their own (a worker, a token) link to the list that holds them;
 * kinds with no page at all stay plain text.
 */
export function AuditTarget({ kind, target }: Props) {
  switch (kind) {
    case "job":
      return (
        <Link to="/jobs/$id" params={{ id: target }} className={LINK}>
          {target}
        </Link>
      );
    case "workflow_run":
      return (
        <Link to="/workflows/$id" params={{ id: target }} className={LINK}>
          {target}
        </Link>
      );
    case "topic":
      return (
        <Link to="/topics/$topic" params={{ topic: target }} className={LINK}>
          {target}
        </Link>
      );
    case "webhook":
      return (
        <Link to="/webhooks/$id/deliveries" params={{ id: target }} className={LINK}>
          {target}
        </Link>
      );
    case "queue":
      return (
        <Link to="/jobs" search={{ page: 0, pageSize: 25, queue: target }} className={LINK}>
          {target}
        </Link>
      );
    case "worker":
      return (
        <Link to="/workers" className={LINK}>
          {target}
        </Link>
      );
    case "token":
      return (
        <Link to="/grpc-tokens" className={LINK}>
          {target}
        </Link>
      );
    default:
      return <span className={PLAIN}>{target}</span>;
  }
}
