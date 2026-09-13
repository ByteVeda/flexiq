// Package step holds the durable-step rules, with nothing else in them.
//
// Identity, the caps, the snapshot codec and one attempt's walk through a
// job's recorded sequence. No gRPC, no connection, no clock it is not handed:
// everything here is a pure function of what it is given, which is what makes
// it testable without a server and reviewable against the Rust original.
//
// It is a reimplementation, not a binding. The native SDK shells hand these
// decisions to flexiq_core::step across their FFI; a Go executor links no Rust
// and so has to make the same ones itself. Every rule here is pinned to the
// file it mirrors, because the two must agree byte for byte — a key derived
// differently is a memo that never matches, and a memo that never matches is a
// charge that runs twice.
//
// Internal on purpose. The step surface a caller uses lives in the executor
// package; these are the rules underneath it, and their shape should stay free
// to change.
package step
