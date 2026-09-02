// The one nav shape every SDK tree must have (#780).
//
// Before this, `guides/` absorbed anything without an obvious home and each SDK
// invented its own groups: python had eleven, node and java nine, and flipping
// the SDK switch redrew the map of the product. The skeleton makes the shape a
// committed artefact instead of a convention — `scripts/parity/checks/
// section-shape.mjs` fails the build when a tree drifts from it.
//
// The contract is deliberately asymmetric:
//
//   - `title` must match exactly. A section is the same section in all three
//     trees or it is a different product.
//   - `pages` must be a SUBSEQUENCE of the list here. An SDK that lacks a topic
//     omits the page; it never renames it, reorders it, or invents a group for
//     it. A page this file does not list is an error even if the MDX exists —
//     adding a topic is a decision about all three trees at once.
//
// `dir` is relative to the SDK root. Sections outside a tree (`architecture`,
// `about`) are shared by construction and need no skeleton. So is
// `api-reference/symbols`, which `scripts/api/` generates.

export const SECTION_SKELETON = [
  {
    dir: "getting-started",
    title: "Getting Started",
    pages: ["installation", "quickstart", "concepts", "capabilities"],
  },
  {
    dir: "guides",
    title: "Guides",
    pages: ["index", "core", "reliability", "extend"],
  },
  {
    dir: "guides/core",
    title: "Tasks & execution",
    pages: [
      "index",
      "tasks",
      "discovery",
      "queues",
      "workers",
      "execution-model",
      "enqueue-options",
      "scheduling",
      "cancellation",
      "predicates",
      "batching",
      "batch-enqueue",
      "unique-tasks",
      "dependencies",
      "streaming",
      "async-tasks",
      "prefork",
      "pubsub",
    ],
  },
  {
    dir: "guides/reliability",
    title: "Reliability",
    pages: [
      "index",
      "retries",
      "error-handling",
      "guarantees",
      "timeouts",
      "dead-letter",
      "idempotency",
      "debouncing",
      "flow-control",
      "rate-limiting",
      "concurrency",
      "circuit-breakers",
      "locks",
    ],
  },
  {
    dir: "guides/extend",
    title: "Extend & integrate",
    pages: [
      "index",
      "events",
      "middleware",
      "webhooks",
      "serializers",
      "monitoring",
      "logging",
      "notes",
      "testing",
      "otel",
      "micrometer",
      "prometheus",
      "sentry",
      "flask",
      "fastapi",
      "django",
      "express",
      "fastify",
      "nest",
      "spring",
    ],
  },
  {
    dir: "modules",
    title: "Modules",
    // `server` (gRPC server mode) lands here with #721 — add it between
    // `executor` and `autoscaling` along with the pages, not before them.
    pages: [
      "workflows",
      "steps",
      "dashboard",
      "injection",
      "mesh",
      "executor",
      "autoscaling",
    ],
  },
  {
    dir: "modules/workflows",
    title: "Workflows",
    pages: [
      "index",
      "building",
      "fan-out",
      "conditions",
      "gates",
      "sub-workflows",
      "saga",
      "canvas",
      "caching",
      "analysis",
    ],
  },
  {
    dir: "modules/dashboard",
    title: "Dashboard",
    pages: ["index", "authentication", "sso", "task-overrides", "rest-api"],
  },
  {
    dir: "modules/injection",
    title: "Dependency Injection",
    pages: [
      "index",
      "dependency-injection",
      "proxies",
      "interception",
      "configuration",
      "observability",
      "testing",
    ],
  },
  {
    dir: "modules/autoscaling",
    title: "Autoscaling",
    pages: ["index", "keda"],
  },
  {
    dir: "operate",
    title: "Operate",
    pages: [
      "index",
      "backends",
      "inspection",
      "cli",
      "deployment",
      "security",
      "troubleshooting",
      "migration",
      "upgrading-0.15",
      "graalvm",
    ],
  },
  {
    dir: "api-reference",
    title: "API Reference",
    pages: [
      "index",
      "overview",
      "queue",
      "task",
      "worker",
      "result",
      "context",
      "resources",
      "serializers",
      "workflows",
      "canvas",
      "saga",
      "batching",
      "testing",
      "errors",
      "cli",
      "symbols",
    ],
  },
  {
    dir: "api-reference/queue",
    title: "Queue",
    pages: [
      "index",
      "jobs",
      "queues",
      "workers",
      "resources",
      "events",
      "pubsub",
    ],
  },
  {
    dir: "more/examples",
    title: "Examples",
    pages: [
      "index",
      "fastapi-service",
      "express-service",
      "spring-service",
      "notifications",
      "web-scraper",
      "data-pipeline",
      "workflows",
      "saga-checkout",
      "bulk-emails",
      "predicate-gated-jobs",
      "benchmark",
    ],
  },
];
