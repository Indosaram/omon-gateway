# omo app-server Wire Protocol — Lead-Verified Spec

Written by lead (2026-08-28) after the research child stalled; ground truth = live probe on this machine + local source. Probe artifacts: /tmp/omon-probe/probe.mjs, daemon.log. OmO v5.0.0-0.beta.22 (senpi 2026.8.26-2).

## 1. Transport (proven)

`ws://127.0.0.1:19742` works. Launch: `omo app-server --listen ws://127.0.0.1:19742 --ws-auth off`.
`--ws-auth <token-file|off>` — bearer auth optional; `off` disables. Rust client: **tokio-tungstenite** (ws), JSON-RPC 2.0 messages, one JSON object per WS text frame. Daemon also prints `readyz http://127.0.0.1:19742/readyz` (HTTP readiness probe).

## 2. Framing (captured)

JSON-RPC 2.0: requests `{jsonrpc:"2.0", id, method, params}`, responses `{id, result}` or `{id, error}`, notifications `{method, params}` (no id). Confirmed by source `rpc/ndjson.js` (message has method/id/params) and live capture.

## 3. Session lifecycle (methods + JS cites)

- `initialize` `{clientInfo:{name,title,version}}` → `{userAgent, codexHome, platformOs, ...}` (`protocol/methods.d.ts:1`, runtime.js)
- `thread/start` — params = ThreadRuntimeOverrides & {...} (`protocol/thread.d.ts`): overrides include **`model`, `modelProvider`, `baseInstructions`, `developerInstructions`** ← persona/model injection; response `{thread:{id, sessionId, modelProvider, status, path...}, model, modelProvider}` (`thread.d.ts` ThreadRuntimeResponse). Thread id is a **server-assigned UUID**; no client-chosen id field → gateway must store the returned threadId in `sessions.state_json` and reuse it (deterministic remap happens gateway-side).
- `thread/resume` `{threadId, ...}` → resumes existing thread (`thread.d.ts` ThreadResumeParams) — session continuity across gateway restarts.
- `turn/start` `{threadId, input:[{type:"text",text}], model?, cwd?}` → `{turn:{id, status:"inProgress"}}` (`protocol/turn.d.ts` TurnStartParams).
- `turn/steer` `{threadId, input, expectedTurnId}`, `turn/interrupt` `{threadId, turnId}`.
- Server→client **requests** requiring responses: `execCommandApproval`, `applyPatchApproval`, `item/tool/call`, etc. (`methods.d.ts:6`) — gateway must respond (deny) or run daemon with restrictive approval policy.

## 4. Streaming notifications (captured, in order)

`thread/started` → `turn/started` → `item/started`(userMessage) → `item/completed`(userMessage) → `item/started`(agentMessage) → **`item/agentMessage/delta`** ×N `{threadId, turnId, itemId, delta:"OM"|"OP"|...}` → `item/completed`(agentMessage, full `text`) → `thread/status/changed{idle}` → **`turn/completed`** `{threadId, turn:{id, items:[...], status:"completed"}}`. Deltas carry text fragments; `item/completed` carries the full agent message text. Also available: `item/reasoning/*`, `item/commandExecution/outputDelta`, `thread/tokenUsage/updated`.

## 5. CAPTURED REAL TURN (provider=quotio, daemon ws-auth off)

```
SEND {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"omon-probe","title":"omon-probe","version":"0.1"}}}
RESP {"id":1,"result":{"userAgent":"omon-probe/2026.8.26-2 (Darwin 25.6.0; arm64) senpi_app_server","codexHome":"/Users/indo/.omo/agent",...}}
SEND {"jsonrpc":"2.0","id":2,"method":"thread/start","params":{"developerInstructions":"You are a probe. Follow the user instruction exactly."}}
RESP {"id":2,"result":{"thread":{"id":"01a046e3-3d07-77ed-88b7-6d3a0dd8862f","sessionId":"01a046e3-...","modelProvider":"quotio",...},"model":...}}
SEND {"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"threadId":"01a046e3-3d07-77ed-88b7-6d3a0dd8862f","input":[{"type":"text","text":"Reply with exactly this word and nothing else: OMOPROBE-OK"}]}}
EVENT item/agentMessage/delta {"threadId":"01a046e3-...","turnId":"53e0a2ef-...","itemId":"resp_05829fcb...:0","delta":"OM"}   // then OP,RO,BE,-,OK
EVENT item/completed {"threadId":"01a046e3-...","turnId":"53e0a2ef-...","item":{"type":"agentMessage","text":"OMOPROBE-OK",...}}
EVENT turn/completed {"threadId":"01a046e3-...","turn":{"id":"53e0a2ef-...","items":[...userMessage,agentMessage...],...}}
```
Full text `OMOPROBE-OK` confirmed in `item/completed`. ✔ sentinel captured.

## 6. Rust client sketch

```rust
// tokio-tungstenite + serde_json; per gateway session: reuse stored threadId or thread/start.
let (ws, _) = tokio_tungstenite::connect_async("ws://127.0.0.1:19742").await?;
ws.send(Message::Text(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}).to_string())).await?;
// read loop: match on msg.method {
//   "item/agentMessage/delta" => yield StreamChunk{ delta },
//   "turn/completed" => finish turn,
//   "execCommandApproval" | "applyPatchApproval" => respond denied,
// }
```

## 7. Cleanup receipt

Daemon pid 80671 (ws://127.0.0.1:19742) killed via session teardown; `lsof -i :19742` empty. Probe client session killed. Temp dir /tmp/omon-probe retained until final verification cleanup (contains probe.mjs + daemon.log). Repo root `probe_test.mjs` from the cancelled child is leftover scratch — safe to remove.
