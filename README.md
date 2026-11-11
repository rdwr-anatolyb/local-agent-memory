# agent-memory-bus

A local, shared event log for coding agents, built on NATS JetStream and exposed to each agent as an MCP stdio server (`amb mcp`). The design is in `0-pstack:architect.md`. The agent-facing usage guide is `skills/agent-memory-bus/SKILL.md`.

## Prerequisites

- Bun 1.4 or later.
- `nats-server` on `PATH`, or its path in `AMB_NATS_SERVER_BIN`. On macOS, run `brew install nats-server`.

## Install

```sh
bun install
```

## Run the tests

The proofs start a real `nats-server` on a random localhost port and a temporary home. They fail, never skip, if the binary is missing.

```sh
bun test
bunx tsc --noEmit
```

Set `AMB_TEST_TMP=<dir>` to put the temporary server homes somewhere other than the system temp directory.

## Run the server

```sh
bun src/main.ts server start    # detached, bound to 127.0.0.1:4777, data in ~/.agent-memory-bus
bun src/main.ts server status
bun src/main.ts server stop     # signals only a nats-server whose -sd is this home's; removes a stale pid file
```

`amb mcp` also starts the server on demand. `AMB_URL` and `AMB_HOME` override the address and the data directory.

### Windows (unverified)

The Windows code path is unit-tested on macOS with a fake platform, fake `run`, `kill` and `spawn`. It has not been run on Windows. Only these are established:

- `nats-server.exe` is found through `Bun.which("nats-server.exe")`, or through `AMB_NATS_SERVER_BIN` (set it to the full path of the `.exe`). The missing-binary message names the release page, not brew.
- `server stop` finds a server by querying `Get-CimInstance Win32_Process` through `powershell`, then matches the binary name and the `-sd` store dir case-insensitively.
- `server start` spawns detached with `windowsHide`.

Not verified, and treat each as an open risk:

- Whether the detached server keeps running after `server start` exits (Bun issue oven-sh/bun#31603 is unconfirmed).
- Whether `powershell` and CIM are present, which execution policy applies, and the 1-2 s PowerShell start cost.
- The exact command-line text Windows reports for quoted paths, 8.3 short names and drive-letter case, and whether `nats-server` records `-sd` as passed.
- Whether `Bun.which("nats-server")` finds the bare name on Windows without the `.exe` retry.
- Whether a `-sd` store written with forward slashes is matched the same way.
- `server stop` sends SIGTERM, which Windows treats as an unconditional kill. JetStream recovery after that is untested here.

## Wire an agent

Each agent process needs its own identity in the environment. `amb mcp` refuses to start without `AMB_AGENT_ID`, `AMB_PROJECT` and `AMB_ROLE`. A worker id is unique per attempt: a retried task gets a new id and a fresh cursor.

| Variable | Worker | Planner | Dashboard | Orchestrator |
|---|---|---|---|---|
| `AMB_AGENT_ID` | `w-<story>-<task>-<attempt>` | `planner-<story>` | `dash-<n>` | `orch-<n>` |
| `AMB_ROLE` | `worker` | `planner` | `dashboard` | `orchestrator` |
| `AMB_PROJECT` | project slug | same | same | same |
| `AMB_STORY` | story id | story id | unset | must be unset |
| `AMB_TASK` | task id | unset | unset | must be unset |

The orchestrator (sdlc-flow's engine) acts on every story in its project. It may publish `task_assigned`, the flow events `kill`, `gate_pending`, `gate_decided`, `status_changed` and `child_link`, and lifecycle events (`task_started`, `task_result`, `error`) on behalf of an assignee. It must name that assignee in `to_agent`, and status counts the event only if the orchestrator made that assignment. Only the orchestrator may publish flow events. It sends no heartbeats, so an attempt it starts shows `stalled` 90 s after `task_started`. sdlc-flow must use its own lease for those attempts, not amb's `stalled` state. A retry is a new `task_assigned` with a new attempt number and a new `to_agent`; reusing an `event_id` across attempts makes the bus drop the retry as a duplicate for 24 h.

For sdlc-flow Tasks, `correlation_id` is the Task ID and `task_id` is the node key, both encoded with `encodeToken`. The `publish_event` input refuses unknown keys, so a `project_id` argument is an error, not an override.

## Library entry

`package.json` exports `src/index.ts`, so sdlc-flow can depend on this repo by git and import `MemoryBus`, `readIdentity`, `ensureServer`, `stopServer`, `serverStatus`, the event schema and `encodeToken`/`decodeToken`. Importing the entry starts nothing and prints nothing; `src/main.ts` runs only as the `amb` command. The MCP server (`src/mcp.ts`) is not exported, so library callers do not load the MCP SDK.

`busOverview({ url })` and `busMessages({ url, project?, beforeSeq?, limit? })` inspect a running bus for a status page. Both are read-only and create no reader: they open their own short connection, never a `MemoryBus`, so they add no cursor. `busOverview` returns the stream's counts and retention, the heartbeat count, the server version and each durable cursor (`project`, `agent`, unread, unconfirmed). `busMessages` returns up to `limit` (1 to 200) events, newest first, optionally of one project and before a seq, with `nextBefore` for the next page. Each call reads at most 2000 seqs, so a page may be empty while `nextBefore` is set. Summaries are cut to 160 characters, and details are kept, cut to 200, only for the types with a pointer schema; free-form details never leave the bus.

`encodeToken` maps a Task ID or node key to a bus token. An id made of `[A-Za-z0-9-]` segments joined by `.` (at most 64 characters) becomes the same text with `.` replaced by `_`, and `decodeToken` reverses it. Any other id becomes a readable prefix of at most 50 characters, `__`, and a 12-hex sha256 suffix. `decodeToken` returns `undefined` for those; map them back from the id list you already hold.

Claude Code (`.mcp.json` in the worktree):

```json
{"mcpServers":{"amb":{"command":"bun","args":["/path/to/agent-memory-bus/src/main.ts","mcp"],"env":{
  "AMB_AGENT_ID":"${AMB_AGENT_ID}","AMB_ROLE":"${AMB_ROLE}","AMB_PROJECT":"${AMB_PROJECT}",
  "AMB_STORY":"${AMB_STORY:-}","AMB_TASK":"${AMB_TASK:-}"}}}}
```

Codex (`config.toml`):

```toml
[mcp_servers.amb]
command = "bun"
args = ["/path/to/agent-memory-bus/src/main.ts", "mcp"]
env_vars = ["AMB_AGENT_ID", "AMB_ROLE", "AMB_PROJECT", "AMB_STORY", "AMB_TASK"]
```

Tuning variables, mainly for tests (whole milliseconds, 1 to 2147483647): `AMB_STATUS_WINDOW_MS` (86400000), `AMB_HEARTBEAT_MS` (30000), `AMB_STALL_AFTER_MS` (90000), `AMB_CLAIM_TIMEOUT_MS` (600000), `AMB_QUIET_AFTER_MS` (1800000), `AMB_ACK_WAIT_MS` (30000). An ad hoc agent (`AMB_ROLE=agent`, which must be set explicitly) may also set `AMB_FILTER` (comma-separated subjects) and `AMB_CURSOR_START` (`all` or `new`).

## Delivery semantics

- `read_events` confirms (ACKs) the previous batch at the start of the next call. If a session dies before it reads again, the same identity gets that batch again after `ack_wait`.
- One `read_events` result is capped near 50 KB, measured as the escaped lines that are sent. Everything a call returns is confirmed by the next call; nothing is dropped in between. `truncated=true` means more events are ready.
- An event whose escaped form exceeds 48,000 bytes is refused at publish, so every stored event fits one result.
- Lifecycle events (`task_started`, `task_result`, `error`) must name the publisher's own story and task. One exception: an identity without a task (a planner, an ad hoc agent) may publish `error` about itself, with no task and its own story or none. Status counts lifecycle events only from the assignee of the latest attempt. A worker publishes its own `task_started`; its `amb` process then sends heartbeats.
- A `read_events` call that the client cancels releases its batch, and the next call returns it again. If a header says `confirmed previous batch seq a..b` but you never saw that result, read it with `replay_events from_seq=a own_scope=true`.
- Messages that are not valid events, or whose subject contradicts their own fields, are skipped (reported as `skipped … malformed` by `read_events`). Heartbeats are checked the same way and ignored if their timestamp is more than 5 s ahead of the reader's clock.
- `get_task_context`, `get_decisions` and `replay_events` without `from_seq` keep the newest events when a result is cut; the note gives the `from_seq` for the older ones. `get_status` gives every story it picks an equal share of the result, so each keeps its id, `done`, `lastSeq` and task states; a large story first loses artifact lists, then result summaries, then is listed compactly, and as a last resort lists only some tasks with `tasks_omitted`. Without `correlation_id` it picks at most 50 stories: those with the most recent task activity (assignment, start, result or error) in the window, most recent first. If more were active, the first line says `showing 50 of M stories…`; pass `correlation_id`, or a narrower `since`, for the others. A story cut to `tasks_omitted` gets a note pointing to `replay_events {correlation_id, event_type: "task_assigned"}` for its full task list.
- Ids are not authenticated in stage 1. Any local process can claim any `AMB_AGENT_ID`; the role guards prevent mistakes, not attacks. `AMB_FILTER` must stay inside `amb.ev.<project>.`.
- Deduplication is per project and agent: `event_id` values chosen by different agents never collide.
