# PLAN-01: amb upstream core (library entry, orchestrator role, flow events, token helper, TS 5.9)

Ticket: `sdlc-flow-flow-engine/.scratch/flow-engine/issues/01-amb-upstream-core.md` (spec §9 items 1-5, 7, 8; ADR 0019).
Branch `feat/flow-engine-upstream`. Baseline: `bun test` 158 pass. Plan only; no source changed yet.

## Facts checked before planning

- `bus.publish` already takes `correlation_id` and `task_id` from the caller for every role, and `publish_event` already has both inputs. The project is always stamped from the identity (`build`, bus.ts:183) and there is no project input. Item 5 needs almost no new code.
- amb already type-checks under TypeScript 5.9.3. Both amb's own tsconfig and a scratch tsconfig with sdlc-flow's options (`module: ESNext`, `allowImportingTsExtensions`, `esModuleInterop`) passed with sdlc-flow's `node_modules/.bin/tsc` (5.9.3). The only gap is that amb's devDependency is `typescript ^7.0.2`, so nothing keeps it compatible with 5.9 over time.
- MCP SDK 1.32.2 `registerTool` accepts a full zod object schema (`normalizeObjectSchema`). A `z.strictObject` input therefore refuses unknown keys with an InvalidParams error. A raw shape strips them silently.
- `deriveStatus` attaches an event to an attempt only when `e.agent_id` is that attempt's assignee (status.ts:63). If the server publishes lifecycle events as itself, they would be ignored. Decision 2 fixes this.

## Decisions

### 1. Library entry and `exports`

- New file `src/index.ts`, re-exports only, no top-level statements:
  - `MemoryBus` and the types `PublishInput`, `Query`, `ReadResult`, `TaskContext` from `./bus`
  - `readIdentity` and the types `Identity`, `Role`, `Thresholds` from `./identity`
  - `ensureServer`, `stopServer`, `serverStatus` and the type `StopResult` from `./server` (`serverStatus` feeds `sdlc-flow bus status`, spec §4.1)
  - `AmbEvent`, `EVENT_TYPES`, `LIFECYCLE`, `FLOW_EVENTS`, `Token`, `isToken`, `encodeToken`, `decodeToken` and the types `AmbEventT`, `EventType`, `StoredEvent` from `./event`
  - the types `StoryStatus`, `TaskStatus`, `TaskState` from `./status`
- `mcp.ts` is not exported, so the MCP SDK does not load in sdlc-flow.
- `package.json`: add `"exports": { ".": "./src/index.ts" }`. Keep `bin` and `private`. A git dependency does not need `files`. Bun and TS (`moduleResolution: bundler`) both resolve a `.ts` export target.
- `src/main.ts`: wrap everything after the imports in `if (import.meta.main) { ... }`. The `amb` bin and the test harness (`bun src/main.ts mcp`) run it as the main module, so the CLI does not change. An `import` of the file then does nothing. The CLI stays the only thing that runs on load.

### 2. `orchestrator` role

identity.ts:
- `Role` and `ROLES` gain `"orchestrator"`.
- `AMB_STORY` and `AMB_TASK` must be unset. If either is set, throw `orchestrator acts on every story in its project; unset AMB_STORY and AMB_TASK`. A stamped default story would quietly misfile events for other stories.
- `filter = [`${P}.>`]`, `cursorStart = "all"`, the same as an unbound planner. `AMB_FILTER` is ignored, as it is for planner and worker.

bus.ts `publish` guards, in this order:
1. Dashboard read-only and heartbeat refused: unchanged.
2. A `FLOW_EVENTS` type (kill, gate_pending, gate_decided, status_changed, child_link) from any role other than orchestrator: throw `only AMB_ROLE=orchestrator may publish <type>`.
3. `task_assigned` from a role other than planner or orchestrator: throw `only AMB_ROLE=planner or orchestrator may publish task_assigned`. The bound-planner story check stays. The orchestrator has no story, so that check never fires for it.
4. Lifecycle (`task_started`, `task_result`, `error`):
   - Orchestrator: require `correlation_id`, `task_id` and `to_agent` (the assignee of the attempt it speaks for). Otherwise throw `<type> from an orchestrator needs correlation_id, task_id and to_agent (the attempt's assignee)`.
   - Every other role: the existing own-story and own-task rule, unchanged.
5. Heartbeat side effects: skip `startHeartbeat` when `id.role === "orchestrator"`. One bus serves many tasks, and `hbActive` is one flag per process. `stopHeartbeat` stays harmless.

The orchestrator cannot publish outside its project: `project_id` comes only from `id.projectId`, and every story or task token is still checked by `Token`.

How it differs from the other roles:
- Planner: may be bound to one story, assigns only, and cannot publish flow events or lifecycle for others.
- Worker: bound to one story and task, publishes lifecycle only for itself, and cannot assign.
- Orchestrator: has no story binding, may assign, may publish lifecycle on behalf of a named assignee in any story, and is the only role that may publish flow events.

status.ts, so orchestrator lifecycle events count:
- `Attempt` gains `assigner: string`, set from the agent id of the `task_assigned` event that opened the attempt. An ad hoc attempt sets it to `""`.
- Attach rule:
  1. If the publisher holds an attempt, as today.
  2. Otherwise, if the event is lifecycle, `e.to_agent` holds an attempt, and that attempt's `assigner === e.agent_id`, attach the event to the `to_agent` attempt.
  3. Otherwise, the old fallbacks.
- Only the agent that made the assignment can speak for its assignee. A non-assigner's forged `to_agent` is ignored. Existing H1 tests use `to_agent: null`, so they keep passing.

### 3. New event types (event.ts)

`EVENT_TYPES` gains these 5 names, appended. Add `export const FLOW_EVENTS: ReadonlySet<EventType>` holding the same 5. Each name matches the token pattern, so `subjectFor`, `filterFor` and the subject layout (`amb.ev.<project>.<story>.<task|_>.<agent>.<type>`) stay unchanged.

| type | story (`correlation_id`) | `task_id` | details (strict, added to `STRICT_DETAILS`) |
|---|---|---|---|
| `kill` | killed Task (required) | `_` | `z.strictObject({ by_parent: Id.optional() }).optional()` |
| `gate_pending` | Task (required) | encoded node key (required) | `{ gate_id: z.string().min(1).max(200), doc_ref: z.string().min(1).max(500).optional() }` |
| `gate_decided` | Task (required) | node key (required) | `{ gate_id, decision: z.enum(["approve","reject","retry","drop","cancel"]), feedback_ref: z.string().min(1).max(500).optional() }` |
| `status_changed` | Task (required) | `_` | `{ from: z.string().min(1).max(32).nullable(), to: z.string().min(1).max(32) }` |
| `child_link` | the child Task (required) | `_` | `{ parent: Id }` (spec §4.2: "a child Task's first event names its parent") |

- `gate_id` keeps sdlc-flow's raw `<node key>#<iteration>.<attempt>`. It is payload, not a subject token.
- Text fields are pointers (`*_ref`), never prose. This matches the existing `brief_ref` rule.
- The decision enum covers spec §2.1 (approve or reject) and §2.2 (retry, drop or cancel at a blocked join).
- `superRefine`: a `FLOW_EVENTS` type without `correlation_id` gets the issue `<type> needs a story`. A `gate_*` event without `task_id` gets `<type> needs a node (task_id)`.
- Workers see `kill`, `status_changed` and `child_link` for their story through their existing `<story>._.>` filter. That is how a session learns of a kill.

status.ts treatment: these types never open an attempt or attach to one. The orchestrator holds no attempt, and `to_agent` attachment is limited to `LIFECYCLE`. They only raise the story's `lastSeq`. That is wanted: a `get_status wait_ms` loop wakes on a kill. `statusPage`'s active-story picker still counts only `task_assigned` and lifecycle events. There is no `killed` TaskState. The record of a kill stays the Run log (spec §9, known limits).

### 4. Retry attempts

The orchestrator publishes one `task_assigned` per attempt n, with:
- `to_agent` = an attempt id that sdlc-flow chooses, for example `<encoded node key>-a<n>` (still at most 64 characters; spec §4.2, "attempt is the agent id suffix")
- `details` = `{ brief_ref: "sdlc-flow://<story>/<task>", attempt: n }`

- The `brief_ref` must still equal `sdlc-flow://<encoded Task ID>/<encoded node key>` (`AmbEvent`). The persisted Job spec is looked up by (story, task, attempt), not by the ref.
- `event_id`: leave it unset (fresh UUIDv7 per publish), or derive it from the attempt (`<to_agent>-assign`). Dedupe is keyed `project:agent:event_id` within 24 h, so reusing one event_id across attempts would silently drop the retry. That is the one rule to tell sdlc-flow.
- Republishing the same attempt after a restart, with a new `event_id`, does not reset it (existing N5 logic).
- A new attempt number opens a new attempt, and the old one moves to `superseded`.
- The planner-only rule becomes planner-or-orchestrator (guard 3). Workers and agents are still refused.

### 5. Agent role: Task ID and node key

- Use the existing arguments: `correlation_id` = encoded Task ID, `task_id` = encoded node key. Do not add new fields: the library and MCP already accept them for every semantic type.
- `mcp.ts` changes:
  - Wrap the `publish_event` input shape in `z.strictObject({...})`. A stray `project_id` (or any unknown key) is then refused, not silently dropped. This gives "a publish outside its project is refused".
  - Extend the description: "correlation_id = the Task ID and task_id = the node key, both encoded with encodeToken; flow events (kill, gate_*, status_changed, child_link) are orchestrator-only."
- `bus.ts`: no change for item 5. The project is stamped from the identity, and the `PublishInput` type has no project field, so library callers cannot pass one either.
- Do not narrow the agent role to the 4 types. Ad hoc agents legitimately publish `code_change`, `help_request`, `error` and an unassigned `task_started`. The README and SKILL.md document that, and the H1/M1 test proves it. Spec §4.4 limits what sdlc-flow's sessions publish by instruction. Status safety does not depend on that limit: an agent's lifecycle event counts only for its own attempt.

### 6. Token helper (event.ts)

```ts
const SIMPLE = /^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*$/;
/** sdlc-flow Task IDs and node keys as bus tokens. Simple ids swap `.` for `_` and decode back; others get a hash. */
export function encodeToken(raw: string): string {
  if (raw === "") throw new Error("encodeToken: empty id");
  if (SIMPLE.test(raw) && raw.length <= 64) return raw.replaceAll(".", "_");
  const hash = createHash("sha256").update(raw).digest("hex").slice(0, 12); // from node:crypto
  return `${raw.replaceAll(".", "_").replace(/[^A-Za-z0-9_-]/g, "-").slice(0, 50)}__${hash}`;
}
/** The raw id of a simple token; undefined for a hashed one (contains `__`), which callers map by re-encoding known ids. */
export const decodeToken = (t: string) => (t.includes("__") ? undefined : t.replaceAll("_", "."));
```

Why it has this shape:
- A fully reversible encoding that keeps `-` and maps `.` to `_` is impossible. All 64 token characters would be single-character images already, so no escape sequence would be left that cannot also be literal text.
- Reversibility is therefore exact for simple ids (`[A-Za-z0-9-]` segments joined by `.`, at most 64 characters). This covers every node key (`<index>-<slug>` joined by `.`) and most Task IDs.
- Every other id (one with `_`, a space, non-ASCII, or over 64 characters) becomes a readable prefix of at most 50 characters, then `__`, then a 12-hex sha256 suffix. The total is at most 64.
- Simple outputs never contain `__`, because their segments are non-empty, so the two forms never collide.
- The output always matches `TOKEN_RE` and is never `"_"`. A raw `"_"` becomes `___d2e2adf7177b`.
- The function is pure, so its output is stable across runs and processes.

Pinned vectors (computed with this exact code):

| input | output |
|---|---|
| `1-loop.0-code-review` | `1-loop_0-code-review` |
| `a.b` | `a_b` |
| `a_b` | `a_b__648fa9b31bc7` |
| `My Task.md` | `My-Task_md__4ce4806f957b` |
| `"a".repeat(70) + ".b"` | `"a".repeat(50) + "__88701e7ef31e"` (64 characters) |

### 7. TypeScript target

Change the devDependency to `"typescript": "~5.9.3"` and run `bun install`, which updates `bun.lock`. `bun run typecheck` then is the 5.9 check, so amb cannot drift into TS 7-only syntax.
- This is the smallest change: amb uses no TS 7 feature (verified above), and `module: Preserve` exists since TS 5.4.
- No compiled entry is needed. sdlc-flow type-checks amb's `.ts` sources under its own 5.9.
- Verification step (step 9) re-runs sdlc-flow's tsc over `src/index.ts` with sdlc-flow's compiler options.

## 8. Tests (each fails if its behavior is removed)

All tests use the existing harness: one real `nats-server` per file through `NatsHarness`, with `h.open` for library tests and `h.mcp` for MCP subprocess tests.

New `test/lib.test.ts` (no nats):
- L-1 "importing the library entry or main.ts starts nothing and prints nothing". For each of `src/index.ts` and `src/main.ts`, it runs `Bun.spawnSync([process.execPath, "-e", "await import(<abs path>)"])` with `AMB_HOME` set to a fresh temp dir, and expects exit 0, empty stdout and stderr, and no `nats.pid` in that home. Without the `import.meta.main` guard, main.ts prints its usage and exits 2.
- L-2 "the exports map names the library entry with its API". It imports `pkg.exports["."]` and expects the functions `MemoryBus.open`, `readIdentity`, `ensureServer`, `stopServer`, `encodeToken`, `decodeToken` and the object `AmbEvent`.

`test/event.test.ts`:
- T-1 the pinned vectors in the table above (fails if the algorithm changes).
- T-2 `decodeToken(encodeToken(x)) === x` for simple ids, including `1-loop.0-code-review` and `S-17`. It returns undefined for a hashed form.
- T-3 every output matches `isToken` and is not `"_"`, for each pinned input plus `""` (throws), `"_"`, `"日本"` and a 300-character id. `encodeToken("a_b") !== encodeToken("a.b")`.
- E-1 each flow type accepts its strict details and refuses an extra key (`gate_decided` with `note`).
- E-2 a flow type without `correlation_id` is refused. `gate_pending` without `task_id` is refused. `gate_decided.decision: "maybe"` is refused.
- E-3 `subjectFor` on a `kill` gives `amb.ev.P.S._.orch.kill`.
- I-1 `readIdentity` with `AMB_ROLE=orchestrator` gives the filter `amb.ev.P.>` and `cursorStart: "all"`. With `AMB_STORY` set, it throws.

`test/status.test.ts`:
- S-1 orchestrator lifecycle attaches. Events: `assign(1)` from `orch`, then `task_started` and `task_result` from `orch` with `to_agent: "w-T1-1"`. Expect state `completed` and assignee `w-T1-1`. Fails without the assigner rule.
- S-2 a non-assigner's `to_agent` is ignored. `task_result` from `w-T2-1` with `to_agent: "w-T1-1"` leaves the task `assigned`.
- S-3 flow events change nothing but `lastSeq`. `deriveStatus` with a kill, gate_pending, gate_decided, status_changed and child_link interleaved equals the run without them, except `lastSeq`. A story that has only flow events is not listed.
- S-4 retry through the orchestrator: attempt 1 `x-a1` is started then fails (an error with `to_agent x-a1`), then attempt 2 `x-a2`. Expect `attempt: 2`, `assigned`, and `superseded: [{1, "x-a1", "failed"}]`.

`test/proofs.test.ts`, new describe "flow engine upstream (library)":
- O-1 an orchestrator publishes each flow type, plus `task_assigned` and orchestrator lifecycle, for two different stories. Each is stored under `amb.ev.<its project>.<story>…` (checked by `replay`). A worker, a planner and an agent each get the `only AMB_ROLE=orchestrator` error for `kill`. A worker gets `planner or orchestrator` for `task_assigned`.
- O-2 an orchestrator's lifecycle event without `to_agent` is refused. After the orchestrator's `task_started` it sends no heartbeat (`lastHeartbeat` on `amb.hb.<p>.<s>.<t>.<orch>` stays 0).
- O-3 retry attempts end to end. The orchestrator assigns attempts 1 and 2 with default event_ids, then publishes `task_started` and a completed `task_result` for attempt 2. `getStatus` gives `completed`, attempt 2, with attempt 1 superseded. A second publish of attempt 2 with a new event_id reports `duplicate: false` and does not reset it.

`test/proofs.test.ts`, in "role scenarios (MCP)":
- A-1 an agent session (`AMB_ROLE=agent`, id `sess-1`) publishes `finding`, `decision`, `question` and `handoff` with `correlation_id: encodeToken("My Task.md")` and `task_id: encodeToken("1-loop.0-code-review")`. `replay_events` returns all 4 with those ids and `project_id` equal to the session's. The same call with `project_id: "other"` returns `isError: true`. A library replay of project `other` is empty. Without `strictObject`, the stray key is dropped and the call succeeds.

The existing L8 test still passes: a strict object still exposes `properties` without `to_task`.

## 9. Steps

Each step is small, uses symbol-level edits, and runs the named tests. Run `bun run typecheck` after every step.

1. `package.json`: set the devDependency `typescript` to `~5.9.3`, then run `bun install`. Run `bun run typecheck && bun test` (158 pass).
2. `src/main.ts`: add the `import.meta.main` guard. Add `src/index.ts` (§1). Add `exports` to `package.json`. Write `test/lib.test.ts` with L-1 and L-2. Run `bun test test/lib.test.ts test/proofs.test.ts`, because the harness still spawns main.ts.
3. `event.ts`: add `encodeToken` and `decodeToken` (`import { createHash } from "node:crypto"`), and export them from index. Add T-1 to T-3. Run `bun test test/event.test.ts test/lib.test.ts`.
4. `event.ts`: extend `EVENT_TYPES`, add `FLOW_EVENTS`, the 5 detail schemas in `STRICT_DETAILS`, and the 2 `superRefine` checks. Add E-1 to E-3. Run `bun test test/event.test.ts test/status.test.ts`.
5. `status.ts`: add `assigner` to `Attempt` and `open()`, and add the `to_agent` attach branch. Add S-1 to S-4. Run `bun test test/status.test.ts`.
6. `identity.ts`: add the orchestrator role and its rules. Add I-1. Run `bun test test/event.test.ts`.
7. `bus.ts` `publish`: add guards 2 to 5 (§2). Import `FLOW_EVENTS`. Add O-1 to O-3. Run `bun test test/proofs.test.ts -t "flow engine upstream|H1|B-N2|P10"`.
8. `mcp.ts`: wrap the `publish_event` input in `z.strictObject` and update its description. Add A-1. Run `bun test test/proofs.test.ts -t "role scenarios"`.
9. Docs and verification:
   - README: add an orchestrator column to the identity table, state the lifecycle-on-behalf rule, and add the token helper.
   - `skills/agent-memory-bus/SKILL.md` line 50: change to "planner or orchestrator", and note that flow events are orchestrator-only.
   - Run the full `bun test` (158 + new), then `bun run typecheck`.
   - Run sdlc-flow's `node_modules/.bin/tsc -p <scratch tsconfig>`. The scratch tsconfig copies sdlc-flow's compilerOptions with `files: [".../src/index.ts"]` and `typeRoots` pointing at amb's `node_modules/@types`. Expect exit 0 and no output.

## 10. Risks

- **Heartbeats for orchestrator attempts are out of scope.** `heartbeatFrom` keys by the publisher, and the orchestrator does not beat. An attempt the orchestrator starts shows `stalled` 90 s after `task_started`. sdlc-flow must use its own lease (the `next` poll) and not amb's `stalled` or `quiet` for these attempts. Alternatively, a follow-up adds an orchestrator heartbeat on the assignee's `hb` subject. Tell the sdlc-flow ticket.
- **Ids are claims (stage 1).** Anyone can open an `orchestrator` identity and speak for assignees. The assigner rule stops mistakes, not attacks. Kill and gate decisions stay authoritative on HTTP and in the Run log (spec §9).
- **event_id reuse across attempts silently drops a retry** within 24 h (dedupe). Document it in the sdlc-flow integration.
- **`brief_ref` is fixed to `sdlc-flow://<story>/<task>`.** Spec §4.4's `<taskId>/<jobId>` works only if the job is the node key. The attempt is carried in `details.attempt` and `to_agent`, not in the ref.
- **Hashed tokens cannot be decoded.** sdlc-flow maps them back by re-encoding known Task IDs from its files, which are the record of truth. A 48-bit hash collision inside one project is possible but negligible.
- **The decision and status enums lock in vocabulary.** A new gate decision needs an amb release and a pin bump. `status_changed` keeps free strings for that reason.
- **Strict `publish_event` input** refuses unknown keys that clients previously sent harmlessly. Any client that sends extras breaks. No known client does.
- **`bun install` for TS 5.9** may need network access if 5.9.3 is not in the Bun cache. If it cannot be installed, keep TS 7 and add `"typecheck:5.9"` running `bunx -p typescript@5.9.3 tsc --noEmit`.
- **Existing stored streams** need no migration: the types are additive, and old readers skip unknown types as invalid.
- **Old amb readers on the same bus** (a pinned older amb mcp) report the new events as `invalid` and skip them. That is safe but noisy until every reader updates.
