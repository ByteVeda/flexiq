# Lessons

## Changing an error string is a code change — grep for its assertions

**2026-08-28, PR #753.** Reworded the PyO3 debounce guard from
`"debounce_replace_payload requires …"` to `"debounce_replace_payload and
debounce_max_pending require …"`. CI red on Python 3.10:
`test_replace_payload_alone_is_refused_at_the_binding` matched the old prefix.

Two failures, not one:

1. **The string was treated as prose, not as an interface.** Any user-visible
   message a test can `match=` on is part of the contract. Before changing one,
   `grep -rn "<distinctive fragment>"` across tests in every SDK.
2. **The verification loop narrowed after the first green run.** The full Python
   suite ran before the first push; the follow-up commit only re-ran the one
   file that had been edited. A change in `crates/` reaches every shell — re-run
   the full suite of any shell whose binding changed, not the file being worked
   on.

Fix was not to restore the old wording: the message legitimately names two
fields now. The existing test absorbed the second case and the near-duplicate
added in `test_admission.py` was dropped — a binding-boundary debounce test
belongs in `test_debounce.py`.

## A graceful streaming listener needs something to end its streams

**2026-09-03, #720.** The gRPC executor door compiled, passed clippy and passed
every unit test, and then every end-to-end test hung — not in an assertion, but
in teardown. Two separate causes, both invisible below the integration level:

1. **The listener was joined before the thing that ends its streams ran.** An
   attach stream is an in-flight gRPC request, and `serve_with_incoming_shutdown`
   waits for one. The stream only ends when the dispatcher closes the
   connection, and that happened *after* the roles were joined. A circle neither
   side can leave.
2. **An HTTP/2 stream is open until *both* halves close.** The server ending its
   response does not end the call. A client that keeps its request half open —
   because it froze, or because a test held the sender — keeps the listener
   waiting for it.

The rules to carry forward:

- **Adding a long-lived stream to a listener changes its shutdown, always.**
  Before writing the handler, ask what closes the stream and whether that thing
  runs before or after the listener is joined.
- **Never let a graceful shutdown be unbounded.** It is a hang in production,
  where it reads as a `SIGKILL` rather than an error. A grace period after the
  signal costs nothing and turns a deadlock into a warning.
- **A second sender on a response channel is a stream that will not end.** The
  refusal path kept a `tx.clone()` for the life of the connection; the stream
  stayed open until the client hung up. Drop it the moment it cannot be used.
- **Teardown is part of the test.** Every one of these failed at `stop()`, not
  at an assertion. A harness whose teardown is unbounded reports a deadlock as
  a timeout in whatever ran next.

