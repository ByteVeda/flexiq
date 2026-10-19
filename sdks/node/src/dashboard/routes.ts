import type { Queue } from "../index";
import type { RequestContext } from "./auth";
import * as auth from "./auth";
import * as h from "./handlers";

export interface Route {
  method: string;
  pattern: RegExp;
  handle: (
    queue: Queue,
    url: URL,
    params: string[],
    body: unknown,
    ctx: RequestContext,
  ) => unknown | Promise<unknown>;
}

const id = (params: string[]): string => params[0] ?? "";

/** Route table, walked in order: exact paths first, then parameterized ones. */
export const routes: Route[] = [
  { method: "GET", pattern: /^\/api\/auth\/status$/, handle: (q) => auth.authStatus(q) },
  {
    method: "GET",
    pattern: /^\/api\/auth\/whoami$/,
    handle: (q, _u, _p, _b, c) => auth.whoami(q, c),
  },
  {
    method: "POST",
    pattern: /^\/api\/auth\/setup$/,
    handle: (q, _u, _p, body) => auth.setup(q, body),
  },
  {
    method: "POST",
    pattern: /^\/api\/auth\/login$/,
    handle: (q, _u, _p, body) => auth.login(q, body),
  },
  {
    method: "POST",
    pattern: /^\/api\/auth\/logout$/,
    handle: (q, _u, _p, _b, c) => auth.logout(q, c),
  },
  {
    method: "POST",
    pattern: /^\/api\/auth\/change-password$/,
    handle: (q, _u, _p, body, c) => auth.changePassword(q, body, c),
  },
  { method: "GET", pattern: /^\/api\/auth\/providers$/, handle: () => h.authProviders() },
  { method: "GET", pattern: /^\/api\/stats$/, handle: (q) => h.stats(q) },
  { method: "GET", pattern: /^\/api\/stats\/queues$/, handle: (q, url) => h.statsQueues(q, url) },
  { method: "GET", pattern: /^\/api\/queues\/paused$/, handle: (q) => h.queuesPaused(q) },
  { method: "GET", pattern: /^\/api\/jobs$/, handle: (q, url) => h.jobs(q, url) },
  {
    method: "GET",
    pattern: /^\/api\/jobs\/([^/]+)\/errors$/,
    handle: (q, _url, p) => h.jobErrors(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/jobs\/([^/]+)\/logs$/,
    handle: (q, _url, p) => h.jobLogs(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/jobs\/([^/]+)\/replay-history$/,
    handle: (q, _url, p) => h.replayHistory(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/jobs\/([^/]+)\/dag$/,
    handle: (q, _url, p) => h.jobDag(q, id(p)),
  },
  {
    method: "POST",
    pattern: /^\/api\/jobs\/([^/]+)\/replay$/,
    handle: (q, _url, p) => h.replayJob(q, id(p)),
  },
  { method: "GET", pattern: /^\/api\/jobs\/([^/]+)$/, handle: (q, _url, p) => h.job(q, id(p)) },
  { method: "GET", pattern: /^\/api\/dead-letters$/, handle: (q, url) => h.deadLetters(q, url) },
  { method: "GET", pattern: /^\/api\/logs$/, handle: (q, url) => h.logs(q, url) },
  {
    method: "GET",
    pattern: /^\/api\/circuit-breakers$/,
    handle: (q) => h.circuitBreakers(q),
  },
  {
    method: "POST",
    pattern: /^\/api\/dead-letters\/purge$/,
    handle: (q) => h.purgeDeadLetters(q),
  },
  { method: "GET", pattern: /^\/api\/settings$/, handle: (q) => h.listSettings(q) },
  { method: "GET", pattern: /^\/api\/retention$/, handle: (q) => h.retention(q) },
  {
    method: "GET",
    pattern: /^\/api\/retention\/dry-run$/,
    handle: (q) => h.retentionDryRun(q),
  },
  {
    method: "GET",
    pattern: /^\/api\/retention\/override$/,
    handle: (q) => h.getRetentionOverride(q),
  },
  {
    method: "PUT",
    pattern: /^\/api\/retention\/override$/,
    handle: (q, _url, _p, body) => h.putRetentionOverride(q, body),
  },
  {
    method: "DELETE",
    pattern: /^\/api\/retention\/override$/,
    handle: (q) => h.deleteRetentionOverride(q),
  },
  {
    method: "GET",
    pattern: /^\/api\/settings\/(.+)$/,
    handle: (q, _url, p) => h.getSetting(q, id(p)),
  },
  {
    method: "PUT",
    pattern: /^\/api\/settings\/(.+)$/,
    handle: (q, _url, p, body) => h.putSetting(q, id(p), body),
  },
  {
    method: "DELETE",
    pattern: /^\/api\/settings\/(.+)$/,
    handle: (q, _url, p) => h.deleteSetting(q, id(p)),
  },
  { method: "GET", pattern: /^\/api\/scaler$/, handle: (q, url) => h.scaler(q, url) },
  { method: "GET", pattern: /^\/api\/proxy-stats$/, handle: (q) => q.proxyStats() },
  {
    method: "GET",
    pattern: /^\/api\/interception-stats$/,
    handle: (q) => q.interceptionStats(),
  },
  { method: "GET", pattern: /^\/api\/resources$/, handle: (q) => h.resourceStatus(q) },
  { method: "GET", pattern: /^\/api\/tasks$/, handle: (q) => h.listTasks(q) },
  { method: "GET", pattern: /^\/api\/queues$/, handle: (q) => h.listQueues(q) },
  {
    method: "GET",
    pattern: /^\/api\/tasks\/([^/]+)\/override$/,
    handle: (q, _url, p) => h.getTaskOverride(q, id(p)),
  },
  {
    method: "PUT",
    pattern: /^\/api\/tasks\/([^/]+)\/override$/,
    handle: (q, _url, p, body) => h.putTaskOverride(q, id(p), body),
  },
  {
    method: "DELETE",
    pattern: /^\/api\/tasks\/([^/]+)\/override$/,
    handle: (q, _url, p) => h.deleteTaskOverride(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/queues\/([^/]+)\/override$/,
    handle: (q, _url, p) => h.getQueueOverride(q, id(p)),
  },
  {
    method: "PUT",
    pattern: /^\/api\/queues\/([^/]+)\/override$/,
    handle: (q, _url, p, body) => h.putQueueOverride(q, id(p), body),
  },
  {
    method: "DELETE",
    pattern: /^\/api\/queues\/([^/]+)\/override$/,
    handle: (q, _url, p) => h.deleteQueueOverride(q, id(p)),
  },
  { method: "GET", pattern: /^\/api\/middleware$/, handle: (q) => h.listMiddleware(q) },
  {
    method: "GET",
    pattern: /^\/api\/tasks\/([^/]+)\/middleware$/,
    handle: (q, _url, p) => h.getTaskMiddleware(q, id(p)),
  },
  {
    method: "PUT",
    pattern: /^\/api\/tasks\/([^/]+)\/middleware\/([^/]+)$/,
    handle: (q, _url, p, body) => h.putTaskMiddleware(q, id(p), p[1] ?? "", body),
  },
  {
    method: "DELETE",
    pattern: /^\/api\/tasks\/([^/]+)\/middleware$/,
    handle: (q, _url, p) => h.deleteTaskMiddleware(q, id(p)),
  },
  { method: "GET", pattern: /^\/api\/metrics$/, handle: (q, url) => h.metrics(q, url) },
  {
    method: "GET",
    pattern: /^\/api\/metrics\/timeseries$/,
    handle: (q, url) => h.timeseries(q, url),
  },
  { method: "GET", pattern: /^\/api\/workers$/, handle: (q) => h.workers(q) },
  {
    method: "POST",
    pattern: /^\/api\/jobs\/([^/]+)\/cancel$/,
    handle: (q, _url, p) => h.cancel(q, id(p)),
  },
  {
    method: "POST",
    pattern: /^\/api\/dead-letters\/([^/]+)\/retry$/,
    handle: (q, _url, p) => h.retryDead(q, id(p)),
  },
  {
    method: "POST",
    pattern: /^\/api\/queues\/([^/]+)\/pause$/,
    handle: (q, _url, p) => h.pause(q, id(p)),
  },
  {
    method: "POST",
    pattern: /^\/api\/queues\/([^/]+)\/resume$/,
    handle: (q, _url, p) => h.resume(q, id(p)),
  },
  { method: "GET", pattern: /^\/api\/event-types$/, handle: (q) => h.eventTypes(q) },
  {
    method: "GET",
    pattern: /^\/api\/workflows\/runs$/,
    handle: (q, url) => h.workflowRuns(q, url),
  },
  {
    method: "GET",
    pattern: /^\/api\/workflows\/runs\/([^/]+)\/dag$/,
    handle: (q, _url, p) => h.workflowDag(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/workflows\/runs\/([^/]+)\/children$/,
    handle: (q, _url, p) => h.workflowChildren(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/workflows\/runs\/([^/]+)$/,
    handle: (q, _url, p) => h.workflowRun(q, id(p)),
  },
  { method: "GET", pattern: /^\/api\/webhooks$/, handle: (q) => h.webhooks(q) },
  {
    method: "POST",
    pattern: /^\/api\/webhooks$/,
    handle: (q, _url, _p, body) => h.createWebhook(q, body),
  },
  {
    method: "POST",
    pattern: /^\/api\/webhooks\/([^/]+)\/test$/,
    handle: (q, _url, p) => h.testWebhook(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/webhooks\/([^/]+)\/deliveries$/,
    handle: (q, url, p) => h.webhookDeliveries(q, id(p), url),
  },
  {
    method: "GET",
    pattern: /^\/api\/webhooks\/([^/]+)\/deliveries\/([^/]+)$/,
    handle: (q, _url, p) => h.webhookDelivery(q, id(p), p[1] ?? ""),
  },
  {
    method: "POST",
    pattern: /^\/api\/webhooks\/([^/]+)\/deliveries\/([^/]+)\/replay$/,
    handle: (q, _url, p) => h.replayWebhookDelivery(q, id(p), p[1] ?? ""),
  },
  {
    method: "POST",
    pattern: /^\/api\/webhooks\/([^/]+)\/rotate-secret$/,
    handle: (q, _url, p) => h.rotateWebhookSecret(q, id(p)),
  },
  {
    method: "GET",
    pattern: /^\/api\/webhooks\/([^/]+)$/,
    handle: (q, _url, p) => h.webhook(q, id(p)),
  },
  {
    method: "PUT",
    pattern: /^\/api\/webhooks\/([^/]+)$/,
    handle: (q, _url, p, body) => h.updateWebhook(q, id(p), body),
  },
  {
    method: "DELETE",
    pattern: /^\/api\/webhooks\/([^/]+)$/,
    handle: (q, _url, p) => h.deleteWebhook(q, id(p)),
  },
];
