# Contributing to Omon Gateway

Thank you for helping improve Omon Gateway. Contributions should preserve its reliability, security boundaries, and asynchronous performance characteristics.

## Development Setup

1. Install Rust 1.85 or newer with `rustup`.
2. Fork and clone the repository.
3. Create a focused branch from the default branch.
4. Copy `.env.example` to `.env` only when a live Discord or provider integration is needed. Never commit credentials.
5. Build and test the project:

```bash
cargo build
cargo test --all-targets
```

Most unit and integration tests do not require live service credentials.

## Code Guidelines

- Write idiomatic Rust 2021 and keep `rustfmt` output unchanged.
- Keep public documentation, code comments, errors, and user-facing messages in professional English.
- Preserve per-session ordering and avoid blocking operations on Tokio executor threads.
- Prefer bounded channels, explicit timeouts, and deterministic synchronization over sleeps or timing-dependent tests.
- Keep tool filesystem access rooted beneath the configured workspace.
- Propagate errors with useful context; do not silently discard failures.
- Avoid unrelated refactors in focused changes.
- Add or update tests for behavior changes. Tests must not depend on external network services unless explicitly marked and isolated.

## Required Checks

Run all checks before opening a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release
```

Clippy warnings are treated as errors. Do not suppress a diagnostic unless the exception is narrow, documented, and technically necessary.

## Pull Request Flow

1. Open an issue first for significant behavior, protocol, storage-schema, or security changes.
2. Keep each pull request focused on one coherent outcome.
3. Use a clear title and explain the problem, approach, compatibility impact, and verification performed.
4. Include migration notes when changing SQLite schemas or environment configuration.
5. Confirm that no tokens, local databases, logs, generated build output, or private paths are included.
6. Respond to review comments with additional commits; maintainers may squash commits when merging.
7. Obtain passing continuous-integration checks and maintainer approval.

## Commit Messages

Use concise imperative messages. Conventional Commit prefixes are encouraged, for example:

```text
feat: add session routing metric
fix: preserve markdown fences across message chunks
test: cover duplicate delivery handling
docs: clarify multi-bot configuration
```

## Reporting Security Issues

Do not disclose suspected vulnerabilities in a public issue. Follow [SECURITY.md](SECURITY.md) instead.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
