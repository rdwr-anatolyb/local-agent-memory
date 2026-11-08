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

## Wire an agent

Each agent process needs its own identity in the environment. `amb mcp` refuses to start without `AMB_AGENT_ID`, `AMB_PROJECT` and `AMB_ROLE`. A worker id is unique per attempt: a retried task gets a new id and a fresh cursor.

| Variable | Worker | Planner | Dashboard |
|---|---|---|---|
| `AMB_AGENT_ID` | `w-<story>-<task>-<attempt>` | `planner-<story>` | `dash-<n>` |
| `AMB_ROLE` | `worker` | `planner` | `dashboard` |
| `AMB_PROJECT` | project slug | same | same |
| `AMB_STORY` | story id | story id | unset |
| `AMB_TASK` | task id | unset | unset |

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
