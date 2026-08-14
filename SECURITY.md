# Security Policy

## Supported Versions

Security fixes are provided for the latest released minor version of Omon Gateway. Users should upgrade to the newest available release before reporting an issue that may already be resolved.

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |
| Earlier or unreleased revisions | No |

## Reporting a Vulnerability

Do not open a public GitHub issue for a suspected vulnerability.

Report vulnerabilities privately by emailing **security@omon.ai**. Include:

- A concise description of the issue and its security impact
- Affected version, commit, and deployment environment
- Reproduction steps or a minimal proof of concept
- Relevant logs or traces with credentials and personal data removed
- Any known mitigations or evidence of active exploitation
- A secure contact method if encrypted follow-up is required

You should receive an acknowledgement within three business days. The maintainers will validate the report, determine severity, coordinate a fix and release, and provide status updates when material information changes. We aim to provide an initial remediation plan within ten business days, although complex or dependency-level issues may require more time.

Please allow a reasonable remediation period before public disclosure. The project will credit reporters who request attribution unless legal, privacy, or safety constraints prevent it.

## Scope

Reports are especially valuable for:

- Discord authentication, authorization, or allowlist bypasses
- Workspace escape, path traversal, or unintended file access
- Command execution outside configured tool boundaries
- Approval-guard bypasses or forged interaction handling
- Secret exposure in logs, errors, or persisted records
- SQLite injection, corruption, or cross-session data access
- Session isolation, message deduplication, or routing failures with security impact
- MCP, browser, web-fetch, or cron behavior that crosses documented trust boundaries
- Denial-of-service conditions caused by unbounded resource consumption

Third-party services and vulnerabilities that require an already compromised host are generally outside project scope, but maintainers welcome reports when Omon Gateway materially increases the impact.

## Deployment Guidance

Omon Gateway intentionally provides agents with powerful terminal, file, browser, network, and scheduling tools. Operators must:

- Run the service under a dedicated, unprivileged operating-system account.
- Set `DISCORD_ALLOWED_USERS` for any bot reachable by untrusted users.
- Configure `OMON_WORKSPACE_ROOT` to a dedicated directory containing no secrets.
- Mount only required paths into containers and keep the Docker socket inaccessible.
- Protect `.env`, Discord tokens, model-provider keys, databases, logs, and backups.
- Keep Rust dependencies, the runtime image, Chrome, and the host operating system current.
- Review model tool calls and use approval controls appropriate to the deployment.

Never include live credentials in a vulnerability report or public reproduction repository.
