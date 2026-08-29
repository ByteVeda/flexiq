# #696 — an empty debounce placeholder makes a shared window

## Problem
A debounce key template resolves per enqueue. Python and node check only the
*assembled* key, so an empty substitution still leaves a non-empty key:
`"report:{user_id}"` with an empty `user_id` becomes `"report:"`, one window
every caller in that state shares. That is the silent global key a
payload-derived key exists to avoid — it surfaces only as missing runs. Java
rejects it per placeholder (`DebounceKeys.lookup`, #694), so the shells disagree.

## Design
Move the check from the key to the segment, in both shells, mirroring java's
message ("… which is empty — a key segment must carry a value").

- Node: `renderPlaceholder` throws on `""`, next to the guard that already
  refuses an object or a non-finite number.
- Python: substitution runs through `str.format`, which offers no per-field
  hook, so a private `string.Formatter` subclass checks each *rendered* field.
  The name is carried from `get_field` because `format_field` sees the value
  alone. The assembled-key guard stays as defence for a direct caller.
- A key with no placeholder at all stays legal in every shell: a deliberate
  single window, not an accident.

## Checklist
- [x] node: guard in `renderPlaceholder` + test
- [x] python: `_KeySegmentFormatter` + test (parametrized over `{user_id}` and
      `report:{user_id}` — the second is the case the old guard missed)
- [x] docs: the debouncing guide's list of rejected placeholders
- [x] verify: both tests red before the guard, full pytest + vitest after,
      ruff/mypy/biome/tsc

## Review

**One guard each, not one guard plus a rewrite.** Node's is three lines inside
the `case "string"` it already had. Python's is a class only because
`template.format(...)` resolves every field in one call: `Formatter.vformat`
walks the same fields with the same errors (`KeyError`/`IndexError` still reach
the existing handler), so the surrounding code is untouched.

**The old python test asserted the weaker rule.** `test_key_resolving_to_empty_raises`
used the bare template `"{user_id}"`, which the whole-key guard already caught —
the prefixed template was the actual gap. It became one parametrized test over
both shapes rather than a near-duplicate alongside the new one.

**Error string is an interface.** The python message changed from "resolved to
an empty key" to "… is empty"; `grep`ed every `match=`/`toThrow` in all three
SDKs first — the only other hits on "empty key" belong to durable steps.
