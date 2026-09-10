# `fq` — a standalone CLI over the producer door (#832)

Spec: `tasks/specs/2026-09-10-fq-cli-design.md`
Plan: `tasks/plans/2026-09-10-fq-cli.md`

- [ ] 1. Crate skeleton and generated client
- [ ] 2. Endpoint parsing, transport and the bearer credential
- [ ] 3. Shell tokens to `StructuredArgs`
- [ ] 4. Table and proto3 JSON rendering
- [ ] 5. Errors that name the wire's reason
- [ ] 6. `fq enqueue`
- [ ] 7. `fq jobs list`, `get` and `cancel`
- [ ] 8. `fq queues`
- [ ] 9. End to end against a real listener
- [ ] 10. Ship it in the server's release
- [ ] 11. Document it, and the holes
- [ ] 12. Whole-repository verification
