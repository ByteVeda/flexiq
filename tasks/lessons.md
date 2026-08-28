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
