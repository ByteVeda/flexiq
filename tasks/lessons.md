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


## A commit is not done until `git log` says so

Nine commits on the `fq` CLI branch were reported as landed when only one had.
`cargo fmt --all --check` was rejecting each of them, `git commit` exited
non-zero, and the command was written as:

```bash
git add … && git commit -m "…"; git log -1 --format='%h %s'
```

The `;` before `git log` means the shell's exit status is `git log`'s, which is
always zero. The tail of the output showed a commit subject — the *previous*
commit's — and it read as success. The whole sequence had to be rebuilt.

The rules to carry forward:

- **Never chain a reporting command after a mutating one with `;`.** Use `&&`,
  so a failure short-circuits, or capture `$?` immediately after the mutation
  and print it.
- **Verify the postcondition, not the output.** `git log -1` right after a
  commit proves nothing unless you compare its subject to the one you just
  wrote. `git rev-list --count` before and after is unambiguous.
- **A pre-commit hook rejecting the commit is the normal case, not the odd
  one.** `cargo fmt` and `cargo clippy` run on the whole workspace and take
  minutes; backgrounding the commit hides their output, so read the hook log
  rather than the exit code of whatever ran last.
- **Rebuilding a split history is cheap if the files are parked.** Copy every
  not-yet-committed file to a directory outside the tree first, then
  `git reset --hard` and replay. Pre-commit stashes unstaged *tracked* changes
  only, so anything untracked is at risk during a hook run anyway.

## A here-string is not `printf`

**2026-09-16, #844.** A commit on the push-dispatch branch documented reproducing a pinned HMAC
signing vector with `openssl dgst -sha256 -hmac … <<< "$STS"`. A here-string appends a trailing
newline to whatever it feeds a command's stdin, so that reproduction hashed a different string
than the six-field, no-trailing-newline one the string-to-sign actually pins. Running both forms
side by side against the same secret produced two different digests —
`ae8e04b171f581a8d602ac9b2c074c06993423f7ebf8932c70bd5af2bdc30933` from `printf '%s'`, matching the
pinned test vector, and a different one from the here-string. In a signing contract, that kind of
mismatch reads as the scheme itself being broken, not as a shell quoting habit — the reader has no
way to tell the two apart from the digest alone.

The rules to carry forward:

- **Never write a command into a comment or a doc without running exactly that command first.** A
  reproduction step that has not been run is a guess wearing a code block.
- **When a comment or doc asserts that two things produce the same output, produce both and diff
  them.** Printing one and trusting the other by inspection is exactly the gap a here-string's
  extra newline hides in.

## Clippy cannot see a stale `#[allow]`

**2026-09-16, #843/#844.** An `#[allow(dead_code)]` whose comment names the commit that will wire
the item up is a good pattern while that commit is still pending — but a redundant `allow` is not
a warning, so `-D warnings` passing on a later commit says nothing about whether the named commit
actually landed and the marker was removed with it. Counting `git log -p` over the push-dispatch
branch's twenty-seven commits: the attribute was added twenty-six times and removed twenty-five,
in uneven bursts — one commit added twelve markers ahead of the code that would use them, and a
different, later commit removed eleven. Every commit in between passed a fully green
`cargo clippy --all-targets --all-features` carrying markers that had already gone redundant,
because clippy has no lint for an `#[allow]` that permits a warning nothing triggers anymore.

The rule to carry forward:

- **When a commit wires up an item that was previously unused, grep for its `#[allow(dead_code)]`
  marker and remove it in that same commit.** A green `cargo clippy --all-targets --all-features`
  is not evidence the marker is gone — clippy has nothing to warn about an `allow` that permits a
  warning which no longer fires either way.

## One cargo build job at a time

**2026-09-18, #948.** `CLAUDE.md` says `-j2` on a 13 GB machine, but the user stopped a
`cargo test --workspace -j2` mid-run and gave a tighter rule: **one job, never more.** `-j2`
is not a floor to keep because a file wrote it down — the machine's real budget is the
constraint, and it is the user's to set.

The rule to carry forward:

- **Pass `-j1` to every `cargo build` / `cargo check` / `cargo test` / `cargo clippy` you
  run,** and never run two cargo invocations concurrently. `cargo clippy` takes no `-j`,
  so it needs `CARGO_BUILD_JOBS=1` in the environment instead.
- **The cap belongs on anything that compiles on its own too.** The `cargo-clippy`
  pre-commit hook ran uncapped, which made the rule true of typed commands and false of
  every commit; it now carries `CARGO_BUILD_JOBS=1`.
- The `-j2` in `tasks/plans/*` and in the untracked `CLAUDE.md` is **not** covered: a plan
  is a record of what was run at the time, and rewriting one would falsify it.
