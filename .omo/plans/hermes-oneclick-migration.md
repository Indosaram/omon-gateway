# hermes-oneclick-migration - Work Plan

## TL;DR (For humans)
<!-- Fill this LAST, after the detailed plan below is written, so it summarizes the REAL plan. -->
<!-- Plain English for a non-engineer: NO file paths, NO todo numbers, NO wave/agent/tool names. -->

**What you'll get:** <fill last - deliverables in human terms, 1-2 sentences>

**Why this approach:** <fill last - the one or two load-bearing decisions and why>

**What it will NOT do:** <fill last - 1-3 plain lines mirroring Must NOT have>

**Effort:** <Quick | Short | Medium | Large | XL>
**Risk:** <Low | Medium | High> - <one-line driver>
**Decisions to sanity-check:** <fill last - the few choices worth a human glance>

Your next move: <fill - e.g. approve, or run a high-accuracy review>. Full execution detail follows below.

---

> TL;DR (machine): <1 line - effort, risk, deliverables>

## Scope
### Must have
### Must NOT have (guardrails, anti-slop, scope boundaries)

## Verification strategy
> Zero human intervention - all verification is agent-executed.
- Test decision: <TDD | tests-after | none> + framework
- Evidence: <attemptDir>/task-<N>-hermes-oneclick-migration.<ext> (attemptDir = currentAttemptDir from 'omo-agent-toolkit ulw-loop status --json', .omo/evidence/ulw/<session>/<goalId>/a<attempt>; outside ulw-loop use .omo/evidence/)

## Execution strategy
### Parallel execution waves
> Target 5-8 todos per wave. Fewer than 3 (except the final) means you under-split.

### Dependency matrix
| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |

## Todos
> Implementation + Test = ONE todo. Never separate.
<!-- APPEND TASK BATCHES BELOW THIS LINE WITH edit/apply_patch - never rewrite the headers above. -->
- [ ] 1. <title>
  What to do / Must NOT do: <...>
  Parallelization: Wave <N> | Blocked by: <...> | Blocks: <...>
  References (executor has NO interview context - be exhaustive): <src/path:lines>
  Acceptance criteria (agent-executable): <exact command or assertion>
  QA scenarios (name the exact tool + invocation): happy + failure, Evidence <attemptDir>/task-1-hermes-oneclick-migration.<ext>
  Commit: <Y/N> | <type>(<scope>): <summary>

## Final verification wave
> Runs in parallel after ALL todos. ALL must APPROVE. Surface results and wait for the user's explicit okay before declaring complete.
- [ ] F1. Plan compliance audit
- [ ] F2. Code quality review
- [ ] F3. Real manual QA
- [ ] F4. Scope fidelity

## Commit strategy

## Success criteria
