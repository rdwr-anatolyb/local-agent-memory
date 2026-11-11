import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";
import { EVENTS } from "./bus";

/** The project token of a cursor named exactly `cursor_<n>_<project>_<agentId>` (n = the project's length), or undefined. */
export function projectOfCursor(name: string, agentId: string): string | undefined {
  const m = /^cursor_(\d+)_/.exec(name);
  if (!m || String(Number(m[1])) !== m[1]) return undefined;
  const n = Number(m[1]);
  const start = m[0].length;
  const project = name.slice(start, start + n);
  return project.length === n && name.slice(start + n) === `_${agentId}` ? project : undefined;
}

/**
 * Deletes the durable cursors of `agentId` whose project is not in `keepProjects` and returns their project tokens.
 * Every other consumer (ephemeral, other agents, other formats) is left alone.
 */
export async function pruneCursors({ url, agentId, keepProjects }: { url: string; agentId: string; keepProjects: Iterable<string> }): Promise<string[]> {
  const keep = new Set(keepProjects);
  const nc = await connect({ servers: url, reconnect: false, timeout: 2000 });
  try {
    const jsm = await jetstreamManager(nc);
    const doomed: { name: string; project: string }[] = [];
    for await (const c of jsm.consumers.list(EVENTS)) {
      const project = projectOfCursor(c.name, agentId);
      if (project !== undefined && !keep.has(project)) doomed.push({ name: c.name, project });
    }
    for (const d of doomed) await jsm.consumers.delete(EVENTS, d.name);
    return doomed.map((d) => d.project);
  } finally {
    await nc.close();
  }
}
