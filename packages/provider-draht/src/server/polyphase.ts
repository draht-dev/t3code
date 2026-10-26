/**
 * Draht reports `subagent`/`workflow` tool runs as `PolyphaseDetails` v1, a `/workflow` command's
 * finished run as a `polyphase-workflow` custom message, and `duet_delegate` only as exit codes.
 * Unknown fields are ignored and unknown statuses read as running, so newer Draht degrades.
 */
import type {
  OrchestrationV2Subagent,
  OrchestrationV2SubagentWorkflow,
  OrchestrationV2WorkflowAgent,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const DRAHT_SUBAGENT_TOOL = "subagent";
export const DRAHT_WORKFLOW_TOOL = "workflow";
export const DRAHT_DUET_DELEGATE_TOOL = "duet_delegate";
export const DRAHT_WORKFLOW_MESSAGE = "polyphase-workflow";
const DRAHT_DUET_STATE_ENTRY = "duet-state";
export const DRAHT_DUET_STATUS_KEY = "duet";

const MAX_PROGRESS_CHARS = 200;
const MAX_RESULT_CHARS = 10_000;
const MAX_EPOCH_MILLIS = 8_640_000_000_000_000;

const PolyphaseAgent = Schema.Struct({
  i: Schema.Number,
  label: Schema.String,
  agent: Schema.String,
  status: Schema.String,
  task: Schema.String,
  phase: Schema.optional(Schema.Number),
  cancelReason: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  startedAt: Schema.optional(Schema.Number),
  endedAt: Schema.optional(Schema.Number),
  toolCalls: Schema.optional(Schema.Number),
  tokens: Schema.optional(Schema.Number),
  now: Schema.optional(Schema.String),
  statusLine: Schema.optional(Schema.String),
  output: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});
export type DrahtPolyphaseAgent = typeof PolyphaseAgent.Type;

const PolyphaseDetails = Schema.Struct({
  v: Schema.Literal(1),
  runId: Schema.String,
  kind: Schema.String,
  origin: Schema.String,
  mode: Schema.optional(Schema.String),
  title: Schema.String,
  status: Schema.String,
  startedAt: Schema.Number,
  endedAt: Schema.optional(Schema.Number),
  workflow: Schema.optional(
    Schema.Struct({
      name: Schema.String,
      args: Schema.optional(Schema.String),
      path: Schema.optional(Schema.String),
    }),
  ),
  phases: Schema.optional(Schema.Array(Schema.Struct({ title: Schema.String }))),
  currentPhase: Schema.optional(Schema.Number),
  agents: Schema.Array(PolyphaseAgent),
  log: Schema.optional(Schema.Array(Schema.String)),
  totals: Schema.Struct({ tokens: Schema.Number }),
  resultPreview: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});
export type DrahtPolyphaseDetails = typeof PolyphaseDetails.Type;

const decodePolyphaseDetails = Schema.decodeUnknownOption(PolyphaseDetails);

export function parseDrahtPolyphaseDetails(value: unknown): DrahtPolyphaseDetails | null {
  return Option.getOrNull(decodePolyphaseDetails(value));
}

type SubagentStatus = OrchestrationV2Subagent["status"];
export type DrahtTerminalSubagentStatus = Extract<
  SubagentStatus,
  "completed" | "failed" | "cancelled" | "interrupted"
>;

export function isTerminalDrahtSubagentStatus(
  status: SubagentStatus,
): status is DrahtTerminalSubagentStatus {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted"
  );
}

export function drahtRunStatus(status: string): SubagentStatus {
  switch (status) {
    case "done":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "running";
  }
}

export function drahtAgentStatus(status: string): SubagentStatus {
  switch (status) {
    case "pending":
    case "queued":
    case "starting":
      return "pending";
    case "done":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
    case "skipped":
      return "cancelled";
    default:
      return "running";
  }
}

function text(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function count(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) && value >= 0
    ? Math.trunc(value)
    : undefined;
}

function epochMillis(value: number | undefined): number | undefined {
  const millis = count(value);
  return millis !== undefined && millis <= MAX_EPOCH_MILLIS ? millis : undefined;
}

function clip(value: string | undefined, max: number): string | undefined {
  return value === undefined
    ? undefined
    : value.length > max
      ? `${value.slice(0, max - 1)}…`
      : value;
}

function optional<K extends string, V>(key: K, value: V | undefined): { [P in K]?: V } {
  return (value === undefined ? {} : { [key]: value }) as { [P in K]?: V };
}

/** What a running agent is doing now. */
export function drahtAgentProgress(agent: DrahtPolyphaseAgent): string | undefined {
  return clip(text(agent.now) ?? text(agent.statusLine), MAX_PROGRESS_CHARS);
}

/** A settled agent's output preview, or why it stopped. */
export function drahtAgentResult(agent: DrahtPolyphaseAgent): string | undefined {
  const status = drahtAgentStatus(agent.status);
  const reason =
    status === "cancelled"
      ? agent.status === "skipped"
        ? "Skipped"
        : `Cancelled${agent.cancelReason === undefined ? "" : ` (${agent.cancelReason})`}`
      : undefined;
  const body = text(agent.output) ?? text(agent.error) ?? text(agent.statusLine);
  const result = reason === undefined ? body : body === undefined ? reason : `${reason}: ${body}`;
  return clip(result, MAX_RESULT_CHARS);
}

/** The run's current phase, or else its latest log line. */
export function drahtRunProgress(details: DrahtPolyphaseDetails): string | undefined {
  const phaseIndex = count(details.currentPhase);
  const phase = phaseIndex === undefined ? undefined : text(details.phases?.[phaseIndex]?.title);
  return clip(phase ?? text(details.log?.at(-1)), MAX_PROGRESS_CHARS);
}

export function drahtRunResult(details: DrahtPolyphaseDetails): string | undefined {
  return clip(text(details.resultPreview) ?? text(details.error), MAX_RESULT_CHARS);
}

/** The request a workflow run started from: its saved name and arguments. */
export function drahtRunPrompt(details: DrahtPolyphaseDetails): string {
  const name = text(details.workflow?.name);
  const args = text(details.workflow?.args);
  return name === undefined ? details.title : args === undefined ? name : `${name} ${args}`;
}

function workflowAgentState(status: SubagentStatus): OrchestrationV2WorkflowAgent["state"] {
  switch (status) {
    case "pending":
      return "queued";
    case "running":
    case "idle":
    case "waiting":
      return "running";
    case "completed":
      return "completed";
    default:
      return "failed";
  }
}

function workflowAgent(
  agent: DrahtPolyphaseAgent,
  phaseTitles: ReadonlyMap<number, string>,
): OrchestrationV2WorkflowAgent {
  const index = count(agent.i) ?? 0;
  const status = drahtAgentStatus(agent.status);
  const settled = isTerminalDrahtSubagentStatus(status);
  const startedAt = epochMillis(agent.startedAt);
  const completedAt = settled ? epochMillis(agent.endedAt) : undefined;
  const phaseIndex = count(agent.phase);
  return {
    index,
    label: text(agent.label) ?? `Agent ${index + 1}`,
    state: workflowAgentState(status),
    ...optional("phaseIndex", phaseIndex),
    ...optional("phaseTitle", phaseIndex === undefined ? undefined : phaseTitles.get(phaseIndex)),
    ...optional("model", text(agent.model)),
    ...optional("totalTokens", count(agent.tokens)),
    ...optional("toolCalls", count(agent.toolCalls)),
    ...optional(
      "durationMs",
      startedAt !== undefined && completedAt !== undefined && completedAt >= startedAt
        ? completedAt - startedAt
        : undefined,
    ),
    ...optional("startedAt", startedAt),
    ...optional("completedAt", completedAt),
    ...optional("prompt", text(agent.task)),
    ...optional("result", settled ? drahtAgentResult(agent) : undefined),
  };
}

/**
 * Agents can drop out of a bounded update when a large run no longer fits Draht's byte cap, so
 * members keep their last reported state until the run reports them again.
 */
export function drahtWorkflowProgress(
  details: DrahtPolyphaseDetails,
  previous: OrchestrationV2SubagentWorkflow | undefined,
): OrchestrationV2SubagentWorkflow {
  const phases = (details.phases ?? []).flatMap((phase, index) => {
    const title = text(phase.title);
    return title === undefined ? [] : [{ index, title }];
  });
  const phaseTitles = new Map(phases.map((phase) => [phase.index, phase.title] as const));
  const agents = new Map((previous?.agents ?? []).map((agent) => [agent.index, agent] as const));
  for (const agent of details.agents) {
    const mapped = workflowAgent(agent, phaseTitles);
    agents.set(mapped.index, mapped);
  }
  const startedAt = epochMillis(details.startedAt);
  const endedAt = epochMillis(details.endedAt);
  const toolCalls = Array.from(agents.values()).reduce(
    (total, agent) => total + (agent.toolCalls ?? 0),
    0,
  );
  return {
    ...optional("name", text(details.workflow?.name) ?? text(details.title)),
    runHandles: {
      runId: details.runId,
      ...optional("scriptPath", text(details.workflow?.path)),
    },
    phases,
    agents: Array.from(agents.values()).sort((left, right) => left.index - right.index),
    ...optional("totalTokens", count(details.totals.tokens)),
    ...optional("toolCalls", toolCalls > 0 ? toolCalls : undefined),
    ...optional(
      "durationMs",
      startedAt !== undefined && endedAt !== undefined && endedAt >= startedAt
        ? endedAt - startedAt
        : undefined,
    ),
  };
}

const DuetParticipant = Schema.Struct({
  role: Schema.String,
  modelRef: Schema.String,
  thinkingLevel: Schema.optional(Schema.String),
});
export type DrahtDuetParticipant = typeof DuetParticipant.Type;

const DuetState = Schema.Struct({
  version: Schema.Literal(1),
  enabled: Schema.Boolean,
  strategy: Schema.optional(Schema.String),
  participants: Schema.optional(Schema.Array(DuetParticipant)),
});
export type DrahtDuetState = typeof DuetState.Type;

const decodeDuetState = Schema.decodeUnknownOption(DuetState);

/** The `duet-state` custom entry Draht appends whenever duet mode changes. */
export function parseDrahtDuetStateEntry(entry: unknown): DrahtDuetState | null {
  if (typeof entry !== "object" || entry === null) return null;
  const record = entry as { readonly type?: unknown; readonly customType?: unknown };
  if (record.type !== "custom" || record.customType !== DRAHT_DUET_STATE_ENTRY) return null;
  return Option.getOrNull(decodeDuetState((entry as { readonly data?: unknown }).data));
}

/**
 * The duet state on the active branch of a `get_entries` listing. Draht restores duet mode from
 * the branch without appending an entry, so a resumed session reveals its duet only here.
 */
export function latestDrahtDuetState(entriesData: unknown): DrahtDuetState | null {
  if (typeof entriesData !== "object" || entriesData === null) return null;
  const { entries, leafId } = entriesData as {
    readonly entries?: unknown;
    readonly leafId?: unknown;
  };
  if (!Array.isArray(entries) || typeof leafId !== "string") return null;
  const byId = new Map<string, object>();
  for (const entry of entries) {
    if (typeof entry !== "object" || entry === null) continue;
    const id = (entry as { readonly id?: unknown }).id;
    if (typeof id === "string") byId.set(id, entry);
  }
  const visited = new Set<string>();
  let current: string | null = leafId;
  while (current !== null && !visited.has(current)) {
    visited.add(current);
    const entry = byId.get(current);
    if (entry === undefined) return null;
    const state = parseDrahtDuetStateEntry(entry);
    if (state !== null) return state;
    const parentId = (entry as { readonly parentId?: unknown }).parentId;
    current = typeof parentId === "string" ? parentId : null;
  }
  return null;
}

/** Draht's duet status line: `duet:<strategy> <role>`, or no text when duet is off. */
export function parseDrahtDuetStatus(
  statusText: string | undefined,
): { readonly strategy: string; readonly role: string | undefined } | null {
  const match = /^duet:(\S+)(?: (.+))?$/.exec(statusText?.trim() ?? "");
  return match === null ? null : { strategy: match[1]!, role: text(match[2]) };
}

export function formatDrahtDuetParticipant(participant: DrahtDuetParticipant): string {
  return `${participant.role} (${participant.modelRef}${
    participant.thinkingLevel === undefined ? "" : `:${participant.thinkingLevel}`
  })`;
}

const DuetAssignments = Schema.Struct({
  assignments: Schema.Array(Schema.Struct({ participant: Schema.String, task: Schema.String })),
});
const decodeDuetAssignments = Schema.decodeUnknownOption(DuetAssignments);

export function parseDrahtDuetAssignments(
  args: unknown,
): ReadonlyArray<{ readonly participant: string; readonly task: string }> {
  return Option.match(decodeDuetAssignments(args), {
    onNone: () => [],
    onSome: (value) => value.assignments,
  });
}

const DuetDelegateDetails = Schema.Struct({
  results: Schema.Array(Schema.Struct({ participant: Schema.String, exitCode: Schema.Number })),
});
const decodeDuetDelegateDetails = Schema.decodeUnknownOption(DuetDelegateDetails);

/** Each teammate's exit code from a finished `duet_delegate` call. */
export function parseDrahtDuetExitCodes(details: unknown): ReadonlyMap<string, number> {
  return Option.match(decodeDuetDelegateDetails(details), {
    onNone: () => new Map(),
    onSome: (value) =>
      new Map(value.results.map((result) => [result.participant, result.exitCode] as const)),
  });
}

/** A finished `duet_delegate` result: one `## <role> (<status>)` section per teammate. */
export function parseDrahtDuetSections(resultText: string): ReadonlyMap<string, string> {
  const sections = new Map<string, string>();
  for (const section of resultText.split("\n\n---\n\n")) {
    const match = /^(?:[^\n]*\n\n)?## (.+) \((?:completed|failed)\)\n\n([\s\S]*)$/.exec(section);
    const body = text(match?.[2]);
    if (match !== null && body !== undefined)
      sections.set(match[1]!, clip(body, MAX_RESULT_CHARS)!);
  }
  return sections;
}

/** A running `duet_delegate` update: one `<role>: <activity>` line per active teammate. */
export function parseDrahtDuetActivity(updateText: string): ReadonlyMap<string, string> {
  const activity = new Map<string, string>();
  for (const line of updateText.split("\n")) {
    const separator = line.indexOf(": ");
    if (separator <= 0) continue;
    const progress = text(line.slice(separator + 2));
    if (progress !== undefined) {
      activity.set(line.slice(0, separator), clip(progress, MAX_PROGRESS_CHARS)!);
    }
  }
  return activity;
}

/**
 * A `/workflow` command starts its run in the background and only announces it. Workflow names
 * never contain whitespace, so the name ends at the first space.
 */
export function parseDrahtStartedWorkflow(message: string): string | null {
  return /^Started workflow (\S+)/.exec(message)?.[1] ?? null;
}

/**
 * A polyphase child's permission ask names its requester chain before the child's own call id
 * (`<label>[/<nested>]/<callId>`); the first label is the agent this session started.
 */
export function drahtChildApprovalLabel(detail: unknown): string | undefined {
  if (typeof detail !== "object" || detail === null) return undefined;
  const record = detail as { readonly kind?: unknown; readonly toolCallId?: unknown };
  if (record.kind !== "tool_permission" || typeof record.toolCallId !== "string") return undefined;
  const separator = record.toolCallId.indexOf("/");
  return separator <= 0 ? undefined : record.toolCallId.slice(0, separator);
}
