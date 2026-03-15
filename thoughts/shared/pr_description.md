# PR Description Template

Use this template to fill out PR descriptions.

## What problem does this PR solve?

<!-- Describe the problem or gap this PR addresses. What was broken, missing, or painful? -->

## What changes were made?

<!-- Summarize the key changes. Organize by component if touching multiple areas. -->

## Why this approach?

<!-- Explain any non-obvious design decisions. What alternatives were considered and rejected? -->

## Breaking changes / migration notes

<!-- List any breaking API changes, wire-format changes, or required migration steps. "None" if not applicable. -->

## Changelog entry

<!-- One-line entry suitable for a changelog. Example: "feat: add ROW type serialization support" -->

## How to verify it

<!-- Checklist of steps to verify correctness. Commands should be runnable by reviewers. -->

- [ ] `cargo test -p fluss-rs` — all tests pass
- [ ] `cargo clippy -p fluss-rs -- -D warnings` — no new warnings
- [ ] Manual: review the diff for correctness against the linked plan/research doc
