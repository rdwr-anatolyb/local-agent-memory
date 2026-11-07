# Agent Memory Bus stage 1: TDD build review

## Summary

I built stage 1 as a working prototype in the worktree `/Users/anatolyb/.sdlc-flow/worktrees/feature-skill/local-agent-memory-bus-nats-jetstream`. I installed `nats-server` v2.15.2 with `brew install nats-server`, so every proof ran against a real server bound to 127.0.0.1. The suite has 62 tests, and it passed 62 of 62 on three runs in a row. Proofs P1 to P12 all pass. I also checked the three runtime assumptions the doc flagged as unverified, and all three hold. Running the code found two real bugs, and both are fixed (see Deviations, items 1 and 2). Nothing is committed. No nats-server or `amb mcp` process is left running.

## What was built

All paths are under the worktree root.

- `src/event.ts` holds the zod `AmbEvent` schema. `details` is strict for `task_assigned`, `task_result` and `heartbeat`, and free-form for every other type. The file also has `EVENT_TYPES`, `isToken`, `subjectFor`, `hbSubject` and `filterFor`.
- `src/identity.ts` has `readIdentity(env)`. It sets the filter and cursor start from the role. It refuses to start when `AMB_AGENT_ID` or `AMB_PROJECT` is missing, when a value is a literal `${...}`, when a token is bad, when a worker has no story or task, or when the role is unknown. An empty string counts as unset.
- `src/status.ts` has the pure `deriveStatus(events, heartbeats, now, thresholds)`, which implements the state table in §6.4.
- `src/untrusted.ts` has `wrapEvents`, which builds the §7 envelope.
- `src/server.ts` has `ensureServer`, `stopServer`, `serverStatus` and `findServerBin`. The server is detached, bound to 127.0.0.1, and treats the port bind as the lock.
- `src/bus.ts` has `MemoryBus`:
  - `open`, which ensures both streams and the cursor and refuses a rebind
  - `publish`, with role guards, a 64 KiB cap, `msgID` dedupe, and heartbeat start/stop
  - `read`, which ACKs on read and dedupes by event_id
  - `replay`, `getTaskContext`, `getDecisions`, `getStatus` (with the `since_seq` long-poll) and `cursorStatus`
  - `close`
- `src/mcp.ts` registers the 7 tools from §9. Every tool result that holds events uses the envelope and also returns `structuredContent`. The process shuts down on stdin `end`/`close` and on SIGTERM.
- `src/main.ts` is the CLI: `amb mcp` and `amb server start|stop|status`.
- `test/harness.ts` starts a real nats-server child on a random port with a temp home, and starts `amb mcp` subprocesses through the MCP SDK stdio client. If the binary is missing it throws; it never skips.
- `test/event.test.ts` has 36 pure tests. They cover subjects, filters, tokens, the strict schemas, `readIdentity` (including `${` rejection) and the envelope.
- `test/status.test.ts` has 13 pure tests. They cover every state, the clock edges, superseded attempts, story grouping and `done`.
- `test/proofs.test.ts` has 13 proofs: P1 to P12, plus P9b.
- `skills/agent-memory-bus/SKILL.md` is the usage skill, written against the real tool names. It recommends semantic events and says lifecycle events are usually published by sdlc-flow, Orca or the MCP process. It uses "prefer/usually" wording and states the untrusted-events rule. The MCP server blocks nothing beyond the role guards in the doc.
- `README.md` has install, test, server and agent-wiring instructions.
- The repo also has `package.json`, `bun.lock`, `tsconfig.json` and `.gitignore`. The pinned versions are `@nats-io/*` 3.4.0, `@modelcontextprotocol/sdk` 1.32.2 and zod 4.6.7.

## Test commands and real output

```
bunx tsc --noEmit
  -> TypeScript: No errors found

AMB_TEST_TMP=/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad bun test
  -> 62 pass, 0 fail, 147 expect() calls, 3 files, about 18.4 s   (3 runs in a row, all identical)
  -> [P9] stalled visible 1015-1020 ms after SIGKILL (bound: stall_after 600 + wait_ms 1000)

env -i HOME=$HOME PATH=/usr/bin:/bin bun test test/proofs.test.ts      (nats-server hidden)
  -> error: nats-server not found on PATH or AMB_NATS_SERVER_BIN; install it with: brew install nats-server
  -> 0 pass, 1 fail   (fails loudly, does not skip)
```

The orchestrating session re-ran `bun test` in the worktree after this report: 62 pass, 0 fail, 147 expect() calls, 18.45 s, and no nats-server or amb process was left running.

Red-before-green evidence:
- **Pure modules.** The first runs failed only because the modules did not exist yet. That is a weak red.
- **P5.** This was a real red. Its assertion is that after a server SIGKILL the next read starts at the 7th event. It failed in 3 of 5 runs with `Expected: 7 Received: 6`, because the server lost the last ACK and redelivered seq 6. After I added dedupe by event_id in `read`, it passed 6 of 6 times, then passed in three full-suite runs.

I also ran the real binary by hand with a temp `AMB_HOME` on port 4791:
- `server status` printed `down`.
- `server start` printed `started`. A second `server start` printed `running`, so it is idempotent.
- `lsof` showed the server listening only on `127.0.0.1:4791`.
- Over stdio JSON-RPC, `initialize` worked, then `publish_event` returned `seq 1`, then `read_events` returned the event inside the envelope.
- `AMB_AGENT_ID='${AMB_AGENT_ID}'` exited 1 with "unexpanded placeholder".
- `server stop` printed `stopped`, and `server status` then printed `down`.

## Proofs

| # | Status | Observed |
|---|---|---|
| P1 | pass | The new agent got all 20 sequence numbers in order, and `pending` was 0. |
| P2 | pass | The ack floors were at the 5th and 12th events. The next reads started at the 6th and 13th. |
| P3 | pass | `duplicate` came back as [false, true, true] with one seq, and replay held 1 event. |
| P4 | pass | Reopening the identity twice, so that `consumers.add` ran 3 times with the same config, did not move the ack floor. The next read was the 6th event. |
| P5 | pass, after a fix | After SIGTERM the next read is the 6th event. After SIGKILL the server sometimes redelivers the last ACKed message, and `read` now drops it. All 20 events are still present. |
| P6 | pass | Run end to end through one MCP agent. The output has exactly one `</untrusted_events>`. |
| P7 | pass | A raw fetch without ACK gives `[seq, false]`. After `ack_wait` of 1 s the same seq comes back as `[seq, true]`. `MemoryBus.read` then picks it up and ACKs it. |
| P8 | pass | The planner assigned T1 and T2 before the workers started. Each worker saw only its own task plus the story broadcast. The planner's `get_status` loop reached `done:true` with both results. The dashboard's change signal had no old `task_assigned`, which confirms `New`. The dashboard showed both tasks completed, and its `publish_event` was refused as read-only. |
| P9 | pass | `stalled` was visible about 1016 ms after SIGKILL of the worker's MCP process. Attempt 2 completed. A late `task_result` from attempt 1 left the state at completed/attempt 2, with `superseded` = `[{attempt 1, w-T2-1, failed}]`. |
| P9b | pass | This is the §12 orphan check. Closing `amb mcp`'s stdin made it exit with code 0 on its own. |
| P10 | pass (structural) | (a) An extra `details.instructions` was rejected and nothing was stored. (b) The hostile summary appears only inside the envelope, still a lossless JSON string, and the assignment `details` keys are exactly `brief_ref` and `attempt`. (c) A worker publishing `task_assigned` was refused, and so was a `task_result` for another task. Whether an LLM obeys the text in (b) was **not run**; it is a manual eval, as the doc says. |
| P11 | pass | Reopening with a different `AMB_TASK` gave "identity already bound". The T2 replay returned only T2, and `cursorStatus` did not change. |
| P12 | pass | T3 showed `assigned` at first, then `unclaimed` after 700 ms with `claim_timeout` set to 500 ms. |

## Assumptions the doc flagged as unverified

I checked all three in a scratch script against nats-server 2.15.2.

- **`consumers.add` idempotency.** Calling it again with the same config returns no error, and P4 asserts this. Calling it with a different filter fails with `consumer already exists`, so the server never changes the filter silently. The no-skip design holds.
- **Ordered-consumer `fetch` with `expires` wakes on a new message.** It does. The fetch returned 502 ms after a publish, with a 5000 ms `expires`.
- **`LastPerSubject` with multiple `filter_subjects`.** It works and returns exactly the last message for each subject.

Three more facts came from running the code and changed it:
- A pull `fetch` whose `max_messages` is larger than what is pending waits for the full `expires` time. Because of this, `read` and replay size each batch from `num_pending`, and `read` only long-polls when nothing is pending.
- `expires` must be at least 1000 ms, so `wait_ms` below 1000 is rounded up.
- A SIGKILL of the server can lose the last ACK, as described under P5.

## Deviations from the doc

1. **The cursor name includes the project: `cursor_<project>_<agent>`.** Consumer names are scoped to the stream, not the project. The same `planner-S-17` in two projects could not open, which surfaced as MCP "Connection closed" in P9, P10 and P12. With `_` allowed in tokens the name can be ambiguous in theory. If that happens, the filter check catches it and open fails.
2. **`read` dedupes in memory by event_id** (a bounded set of 10k entries). §6.1 says readers dedupe. Real redelivery made this necessary.
3. **`read` returns `{events, invalid}`, not `StoredEvent[]`.** This is how malformed messages get "reported as invalid".
4. **The envelope escapes every `<` in a JSON line as `<`.** The doc says to replace the literal `untrusted_events`. Escaping is lossless, because `JSON.parse` gives back the original text, and no tag can form inside content.
5. **`subscribe()` was not built.** Nothing in stage 1 calls it.
6. **`filterFor` returns the five-token `*` form** (for example `amb.ev.P.S.*.*.*`). It matches the same subjects as `amb.ev.P.S.>`.
7. **Status semantics where the doc was silent:**
   - `done` means every task is completed. A failed task does not count as done.
   - With no heartbeat yet, the stall clock starts at `task_started`.
   - Attempts are keyed by worker identity: `to_agent` for an assignment, the sender for everything else.
   - A task with no assignment, from an ad hoc agent, shows as attempt 0.
8. **The heartbeat's `attempt` comes from replaying this agent's own `task_assigned`.** `task_started` carries no attempt.
9. **More env knobs:** `AMB_ACK_WAIT_MS`, `AMB_STALL_AFTER_MS`, `AMB_CLAIM_TIMEOUT_MS`, `AMB_QUIET_AFTER_MS` and `AMB_HEARTBEAT_MS`.
10. **An existing stream with a different config is kept, not updated.**
11. **`replay` `limit`:** without `from_*` it keeps the last N; with `from_*` it keeps the first N. The MCP tool defaults to 200.
12. **P7 checks the `redelivered` flag at the raw NATS level,** because `MemoryBus.read` does not expose it.
13. **P9 SIGKILLs the MCP child,** as the §10 table says. The §12 variant, killing the parent, is P9b.
14. **The README runs `bun src/main.ts`.** `package.json` declares a bin `amb`, but that needs `bun link`.

## Still unverified

- Codex env forwarding and Pi MCP wiring.
- How an LLM responds to P10(b). That is a manual eval.
- Two `amb mcp` processes starting the server at the same moment. The port-bind lock is not tested.
- The `cursor_status.gap` retention flag and the `num_waiting` warning. Neither is tested.
- The `quiet` state is covered only by the pure tests.
- The MCP process reconnecting after a server restart. Only the library was tested for this, in P5.

## Risks

- **Dedupe is per process.** If the server is SIGKILLed and an agent then restarts, it can see one already-ACKed event again. Replay recovery and the event_id rule in the skill cover this.
- **Every `get_status` call replays the whole story** and drains `AMB_HB`. The cost grows with history, which is acceptable up to the roughly 10^4 to 10^5 events the doc names.
- **P8, P9 and P12 depend on wall-clock timing.** P9's bound is 2.6 s against about 1.02 s observed. On a loaded CI machine they could fail.
- **There is no auth.** The role guards make mistakes harder but do not provide security, as §11 says.

## Follow-ups

- **Token economy.** I measured the envelope at 214 characters, plus about 373 characters per typical event line (one with a 55-character summary). At about 4 characters per token that is roughly 90 tokens per event; the token figure is a guess. No script was added. A compact projection for `read_events`, dropping `session_id`, null fields and the full timestamp, is the cheap win.
- Run the P10(b) manual LLM eval.
- Smoke-test the Codex and Pi wiring.
- Add a `bun link` or `launchd` setup.
- Stage 2: indexer, archiver, context builder. Stage 3: NATS auth and routing on `to_task`.