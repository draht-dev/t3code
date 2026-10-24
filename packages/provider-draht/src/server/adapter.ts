import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { readToolOutputImage } from "@t3tools/shared/toolOutput";
import * as Predicate from "effect/Predicate";
import {
  ProviderDriverKind,
  type ChatAttachment,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2SubagentWorkflow,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type OrchestrationV2ProviderTurnTokenUsage,
} from "@t3tools/contracts";
import { DrahtSettings } from "../settings.ts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { parseDrahtCompactCommand, type DrahtCompactCommand } from "./commands.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  makeProviderFailure,
  makeProviderRetryTurnItem,
} from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import {
  makeDrahtRpcConnection,
  parseDrahtModelSlug,
  drahtRecordField as recordField,
  drahtRecordNumber as recordNumber,
  drahtRecordString as recordString,
  type DrahtRpcConnection,
  type DrahtRpcRecord,
} from "./rpc.ts";
import {
  buildDrahtRpcLaunch,
  materializeDrahtT3McpExtension,
  resolveDrahtLaunchArgs,
} from "./mcpInjection.ts";
import { DRAHT_FILE_CHANGE_TOOLS } from "./mcpExtensionSource.ts";
import {
  DRAHT_DUET_DELEGATE_TOOL,
  DRAHT_DUET_STATUS_KEY,
  DRAHT_SUBAGENT_TOOL,
  DRAHT_WORKFLOW_MESSAGE,
  DRAHT_WORKFLOW_TOOL,
  drahtAgentProgress,
  drahtAgentResult,
  drahtAgentStatus,
  drahtChildApprovalLabel,
  drahtRunProgress,
  drahtRunPrompt,
  drahtRunResult,
  drahtRunStatus,
  drahtWorkflowProgress,
  formatDrahtDuetParticipant,
  isTerminalDrahtSubagentStatus,
  type DrahtTerminalSubagentStatus,
  latestDrahtDuetState,
  parseDrahtDuetActivity,
  parseDrahtDuetAssignments,
  parseDrahtDuetExitCodes,
  parseDrahtDuetSections,
  parseDrahtDuetStateEntry,
  parseDrahtDuetStatus,
  parseDrahtPolyphaseDetails,
  parseDrahtStartedWorkflow,
  type DrahtDuetParticipant,
} from "./polyphase.ts";

export const DRAHT_PROVIDER = ProviderDriverKind.make("draht");
const DRAHT_DRIVER_KIND = DRAHT_PROVIDER;
const DEFAULT_DRAHT_SETTINGS = Schema.decodeSync(DrahtSettings)({});

/**
 * Sentinel model slug meaning "do not call set_model": Draht resolves the model
 * from the user's own settings.json (`defaultProvider`/`defaultModel`).
 */
const DRAHT_INHERIT_MODEL_SLUG = "default";

const STREAM_FLUSH_MS = 50;
const DRAHT_REQUEST_TIMEOUT_MS = 15_000;
const DRAHT_SESSION_TIMEOUT_MS = 60_000;
const DRAHT_UNSOLICITED_ACTIVITY_ERROR =
  "Draht started agent work outside an active T3 turn. The session was stopped to prevent invisible tool execution.";
const SETTLE_PROBE_MAX_ATTEMPTS = 3;
const SETTLE_PROBE_RETRY_DELAY = Duration.millis(100);

const DrahtProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    // Draht reads DRAHT_PERMISSION_MODE once at startup, so a mode change
    // restarts the process.
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    // Polyphase children forward their permission asks to this session.
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: true,
    // CommandPolicy.ensureRollback requires the snapshot whenever provider
    // rollback is enabled; rollbackThread returns the updated provider thread.
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface DrahtAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: DrahtSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

function contentText(content: unknown): string {
  if (!Array.isArray(content)) {
    return typeof content === "string" ? content : "";
  }
  return content
    .map((block) => {
      if (recordField(block, "type") === "text") return recordString(block, "text") ?? "";
      return "";
    })
    .join("");
}

const MAX_STRUCTURED_TOOL_OUTPUT_BYTES = 64 * 1024;

function isBoundedStructuredToolValue(value: unknown): boolean {
  let bytes = MAX_STRUCTURED_TOOL_OUTPUT_BYTES;
  let nodes = 4_096;
  const visit = (entry: unknown, depth: number): boolean => {
    if (depth > 32 || --nodes < 0 || bytes < 0) return false;
    if (Array.isArray(entry)) {
      bytes -= 2 + entry.length;
      return entry.every((item) => visit(item, depth + 1));
    }
    if (Predicate.isObject(entry)) {
      bytes -= 2;
      for (const key in entry) {
        if (!Object.hasOwn(entry, key)) continue;
        if (key.length > bytes) return false;
        bytes -= Buffer.byteLength(JSON.stringify(key), "utf8") + 2;
        if (!visit(entry[key], depth + 1)) return false;
      }
      return bytes >= 0;
    }
    if (typeof entry === "string" && entry.length > bytes) return false;
    bytes -= Buffer.byteLength(JSON.stringify(entry) ?? "null", "utf8");
    return bytes >= 0;
  };
  return visit(value, 0);
}

function drahtToolOutput(result: unknown, toolName: string): unknown {
  const rawContent = recordField(result, "content");
  const content = Array.isArray(rawContent)
    ? rawContent
    : typeof rawContent === "string"
      ? [{ type: "text", text: rawContent }]
      : [];
  let structured = recordField(result, "structuredContent");
  const details = recordField(result, "details");
  const isMcpResult =
    toolName.startsWith("mcp__") &&
    recordString(details, "server") !== undefined &&
    recordString(details, "tool") !== undefined &&
    Array.isArray(recordField(structured, "content"));
  if (isMcpResult) {
    structured = recordField(structured, "structuredContent");
  }
  if (typeof structured === "string" && structured === contentText(content)) {
    structured = undefined;
  }
  // Native read repeats its image and first text note as structured output.
  // Drop only that exact mirror; an extension's extra metadata stays distinct.
  if (toolName === "read" && Predicate.isObject(structured)) {
    const image = readToolOutputImage(structured);
    const note =
      recordString(
        content.find((block) => recordField(block, "type") === "text"),
        "text",
      ) ?? "";
    if (
      image !== null &&
      Object.keys(structured).every((key) => ["type", "data", "mimeType", "note"].includes(key)) &&
      recordString(structured, "note") === note &&
      content.some((block) => {
        const match = readToolOutputImage(block);
        return match !== null && match.mimeType === image.mimeType && match.data === image.data;
      })
    ) {
      structured = undefined;
    }
  }
  if (structured !== undefined && !isBoundedStructuredToolValue(structured)) {
    return {
      content: [
        ...content,
        {
          type: "text",
          text: "Structured output omitted because it exceeds the stored result limit.",
        },
      ],
    };
  }
  if (
    !isMcpResult &&
    structured === undefined &&
    content.every((block) => recordField(block, "type") === "text")
  ) {
    const text = contentText(content);
    return text.length > 0 ? text : undefined;
  }
  return {
    content,
    ...(structured === undefined ? {} : { structuredContent: structured }),
  };
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: DRAHT_PROVIDER, nativeId, strength };
}

const DRAHT_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

interface DrahtStreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

type DrahtCompactionStatus = "running" | "completed" | "failed" | "cancelled";

interface DrahtCompactionState {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
}

interface DrahtProviderRetryState {
  readonly retry: OrchestrationV2ProviderRetry;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
}

function compactionTitle(status: DrahtCompactionStatus): string {
  switch (status) {
    case "running":
      return "Compacting context...";
    case "completed":
      return "Context compacted";
    case "failed":
      return "Context compaction failed";
    case "cancelled":
      return "Context compaction stopped";
  }
}

interface ActiveDrahtTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  messageOrdinal: number;
  readonly streamItems: Map<string, DrahtStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  interrupted: boolean;
  nativeAborted: boolean;
  sawAgentActivity: boolean;
  adoptedWake: boolean;
  readonly promptMayBeCommandOnly: boolean;
  latestCompactionAfterTokens: number | null;
  lastLiveUsedTokens: number | null;
  settleProbeGeneration: number;
  /** An extension may start compaction immediately after Draht emits agent_settled. */
  settleWhenIdle: boolean;
  sawCompaction: boolean;
  manualCompactInFlight: boolean;
  activeCompaction: DrahtCompactionState | null;
  activeProviderRetry: DrahtProviderRetryState | null;
  failure: ReturnType<typeof makeProviderFailure> | null;
  rejectedPromptFailure: ReturnType<typeof makeProviderFailure> | null;
  stopTreeRefs?: DrahtTurnTreeRefs | null;
}

interface DrahtTurnTreeRefs {
  /** Omitted keeps the synthetic ref; null means the turn left nothing on the active branch. */
  readonly nativeTurnRef: OrchestrationV2ProviderRef | null | undefined;
  readonly leafId: string | null;
  readonly retainedNativeTurnIds: ReadonlyArray<string> | undefined;
}

interface PendingDrahtPrompt {
  readonly nativeRequestId: string;
  readonly method: "select" | "confirm" | "input" | "editor";
  readonly questionId: string;
  readonly approvalKey: string;
  /** Draht's own option for each decision, which must be answered by id. */
  readonly optionIds: Partial<Record<"accept" | "decline", string>>;
  readonly expiresAt: number | null;
  expirationFiber: Fiber.Fiber<void> | null;
  runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
}

/**
 * Draht's permission gate names the gated tool in `detail`; the T3 extension's
 * Supervised edit prompt is titled `Allow <tool>?`.
 */
function drahtApprovalRequestKind(event: DrahtRpcRecord, title: string): "command" | "file-change" {
  const toolName =
    recordString(recordField(event, "detail"), "toolName") ?? /^Allow (\S+)\?$/.exec(title)?.[1];
  return toolName !== undefined &&
    (DRAHT_FILE_CHANGE_TOOLS as ReadonlyArray<string>).includes(toolName)
    ? "file-change"
    : "command";
}

interface DrahtApprovalChoice {
  readonly id: string;
  readonly label: string;
  readonly decision: "accept" | "decline";
}

/** Draht's gate offers its choices in `detail.options`, each stating its own decision. */
function drahtApprovalChoices(detail: unknown): ReadonlyArray<DrahtApprovalChoice> {
  const options = recordField(detail, "options");
  if (!Array.isArray(options)) return [];
  const choices: Array<DrahtApprovalChoice> = [];
  for (const option of options) {
    const id = recordString(option, "id");
    const label = recordString(option, "label")?.trim();
    const nativeDecision = recordString(option, "decision");
    const decision =
      nativeDecision === "approve" ? "accept" : nativeDecision === "deny" ? "decline" : undefined;
    if (id === undefined || !label || decision === undefined) continue;
    if (choices.some((choice) => choice.decision === decision)) continue;
    choices.push({ id, label, decision });
  }
  return choices;
}

function drahtApprovalOptionIds(detail: unknown): Partial<Record<"accept" | "decline", string>> {
  const ids: Partial<Record<"accept" | "decline", string>> = {};
  for (const choice of drahtApprovalChoices(detail)) ids[choice.decision] = choice.id;
  return ids;
}

/** Offer exactly Draht's choices, as long as one of them declines. */
function drahtApprovalOptions(detail: unknown): {
  readonly options?: ReadonlyArray<{
    readonly decision: "accept" | "decline";
    readonly label: string;
  }>;
} {
  const choices = drahtApprovalChoices(detail);
  return choices.some((choice) => choice.decision === "decline")
    ? { options: choices.map(({ decision, label }) => ({ decision, label })) }
    : {};
}

interface DrahtThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActiveDrahtTurn | null;
}

interface DrahtWake {
  readonly state: DrahtThreadState;
  readonly events: Array<DrahtRpcRecord>;
}

/** What a subagent report shows; settling a subagent re-sends its last report. */
interface DrahtSubagentUpdate {
  readonly title: string;
  readonly prompt: string;
  readonly model: string | null;
  readonly status: OrchestrationV2Subagent["status"];
  readonly progress: string | undefined;
  readonly result: string | null;
  readonly workflow: OrchestrationV2SubagentWorkflow | undefined;
  /** Epoch milliseconds Draht reported the run or agent ending. */
  readonly endedAt: number | undefined;
}

interface DrahtDuet {
  readonly enabled: boolean;
  readonly strategy: string | undefined;
  /** The participant answering now; the lead in triage. */
  readonly role: string | undefined;
  readonly participants: ReadonlyArray<DrahtDuetParticipant>;
}

/** Compares and keys values that are already plain JSON data. */
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const DRAHT_DUET_OFF: DrahtDuet = {
  enabled: false,
  strategy: undefined,
  role: undefined,
  participants: [],
};

export const makeDrahtAdapterV2 = Effect.fn("makeDrahtAdapterV2")(function* (
  options: DrahtAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const agentScope = yield* AgentScope;
  const fileSystem = yield* FileSystem.FileSystem;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const { continuationRequests } = options;

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: DRAHT_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DRAHT_PROVIDER,
    getCapabilities: () => Effect.succeed(DrahtProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("DrahtAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      const mcpSession = yield* mcpSessions.read(input.threadId);
      const provideCacheFs = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem>) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: DRAHT_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        );
      // The extension owns both the optional MCP bridge and Draht's permission
      // hook. Materialize it even when this session has no MCP credential so
      // Supervised never silently degrades to unrestricted tool execution.
      const extensionPath = yield* provideCacheFs(
        materializeDrahtT3McpExtension(host.paths.providerStatusCacheDir),
      );
      const resolvedLaunchArgs = resolveDrahtLaunchArgs(options.settings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* protocolError(resolvedLaunchArgs.message);
      }
      const launch = buildDrahtRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment: options.environment,
        mcpSession,
        extensionPath,
        runtimeMode: input.runtimePolicy.runtimeMode,
      });
      const scopedLaunch = yield* agentScope.wrap({
        command: options.settings.binaryPath || "draht",
        args: launch.args,
        name: "draht",
        threadId: input.threadId,
        env: launch.env,
      });
      // Finalizers run in reverse order: reap detached work after terminating Draht.
      const scopeUnit = scopedLaunch.unit;
      const stopScope =
        scopeUnit === undefined ? Effect.void : yield* Effect.cached(agentScope.stop(scopeUnit));
      yield* Effect.addFinalizer(() => stopScope);
      const connection: DrahtRpcConnection = yield* makeDrahtRpcConnection({
        command: scopedLaunch.command,
        args: scopedLaunch.args,
        cwd,
        env: launch.env,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: DRAHT_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      // Detached children can keep stdout open after Draht exits.
      if (scopeUnit !== undefined) {
        yield* connection.exited.pipe(
          Effect.matchEffect({ onFailure: () => stopScope, onSuccess: () => stopScope }),
          Effect.forkIn(scope),
        );
      }
      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: DRAHT_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: DrahtProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const pendingPrompts = new Map<string, PendingDrahtPrompt>();
      const sessionApprovals = new Set<string>();
      // Answering a dialog and terminalizing a turn both publish lifecycle
      // events. Draht can settle immediately after `extension_ui_response`, so
      // serialize the two paths to stop `turn.terminal` from overtaking the
      // dialog's own resolution updates.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: DrahtThreadState | null = null;
      let registrationAttempted = false;
      let lastNativeThreadId: string | null = null;
      let stopRequested = false;
      let unsolicitedActivityDetected = false;
      let pendingWake: DrahtWake | null = null;
      let rollbackBarrier: Deferred.Deferred<
        void,
        ProviderAdapter.ProviderAdapterProtocolError
      > | null = null;
      let closed = false;
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          pendingWake = null;
          if (rollbackBarrier !== null)
            yield* Deferred.fail(
              rollbackBarrier,
              protocolError("Draht session closed during rollback"),
            );
        }),
      );
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      let appliedSessionName: string | null = null;
      const outOfTurnExtensionErrors: Array<DrahtRpcRecord> = [];
      let lastKnownLeaf: string | null = null;
      /**
       * Set when a `get_entries` capture failed. Draht may have advanced past
       * `lastKnownLeaf` since, so the cursor no longer bounds a single turn
       * and the next capture re-syncs it instead of trusting it.
       */
      let leafCursorStale = false;
      // Draht's own configured defaults, captured from the first `get_state` so
      // that selecting the displayed "Draht default" again can restore them. Draht
      // has no "unset" commands, so the baselines have to be replayed
      // explicitly.
      let baselineModel: { provider: string; modelId: string } | null = null;
      let baselineThinking: string | null = null;
      let contextWindow: number | null = null;
      const modelContextWindows = new Map<string, number>();
      let modelsDiscovered = false;
      // Prompt responses carry no id. Keep their session-wide send order and
      // owner so a late ack from a settled turn cannot affect the next turn.
      const pendingPromptResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
      }> = [];
      const pendingCompactResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
      }> = [];

      const compactRpcRecord = (command: DrahtCompactCommand): DrahtRpcRecord =>
        command.customInstructions === undefined
          ? { type: "compact" }
          : { type: "compact", customInstructions: command.customInstructions };

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (
          status: OrchestrationV2ProviderSession["status"],
          lastError: string | null = sessionEntity.lastError,
        ) {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: DRAHT_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (
          state: DrahtThreadState,
          patch: Partial<OrchestrationV2ProviderThread>,
          retainedNativeTurnIds?: ReadonlyArray<string>,
        ) {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: DRAHT_PROVIDER,
            providerThread: state.providerThread,
            ...(retainedNativeTurnIds === undefined ? {} : { retainedNativeTurnIds }),
          });
        });

      const itemOrdinal = (turn: ActiveDrahtTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const request = (record: DrahtRpcRecord, timeoutMs = DRAHT_REQUEST_TIMEOUT_MS) =>
        connection.request(record, timeoutMs);

      const nonNegativeInteger = (input: unknown, key: string): number | undefined => {
        const value = recordNumber(input, key);
        return value === undefined ? undefined : Math.max(0, Math.trunc(value));
      };

      const rememberModelContextWindow = (model: unknown): number | null => {
        const provider = recordString(model, "provider");
        const id = recordString(model, "id");
        const capacity = nonNegativeInteger(model, "contextWindow");
        if (provider !== undefined && id !== undefined && capacity !== undefined && capacity > 0) {
          modelContextWindows.set(`${provider}/${id}`, capacity);
          return capacity;
        }
        return null;
      };

      const lifecycleRequest = (record: DrahtRpcRecord) =>
        request(record, DRAHT_SESSION_TIMEOUT_MS).pipe(
          // A local timeout does not cancel Draht's lifecycle hook. Retire the
          // process before fallback can race its eventual switch/new-session.
          Effect.tapError((error) =>
            Effect.logWarning("Draht session lifecycle request failed", {
              providerSessionId: input.providerSessionId,
              operation: record["type"],
              errorTag: error._tag,
            }),
          ),
          Effect.catchTags({
            DrahtRpcTimeoutError: (error) =>
              connection.terminate.pipe(Effect.andThen(Effect.fail(error))),
          }),
          Effect.onInterrupt(() => connection.terminate),
        );

      const tokenUsageFromStats = (
        stats: unknown,
        fallbackUsedTokens: number | null,
        updatedAt: DateTime.Utc,
      ): OrchestrationV2ProviderTurnTokenUsage | undefined => {
        const contextUsage = recordField(stats, "contextUsage");
        const maxTokens = nonNegativeInteger(contextUsage, "contextWindow");
        const usedTokens =
          nonNegativeInteger(contextUsage, "tokens") ?? fallbackUsedTokens ?? undefined;
        if (usedTokens === undefined || maxTokens === undefined || maxTokens === 0)
          return undefined;

        const totals = recordField(stats, "tokens");
        const inputTokens = nonNegativeInteger(totals, "input");
        const cachedInputTokens = nonNegativeInteger(totals, "cacheRead");
        const outputTokens = nonNegativeInteger(totals, "output");
        return {
          usedTokens,
          maxTokens,
          ...(inputTokens === undefined ? {} : { inputTokens }),
          ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
          ...(outputTokens === undefined ? {} : { outputTokens }),
          updatedAt: DateTime.formatIso(updatedAt),
        };
      };

      const readTokenUsage = (fallbackUsedTokens: number | null, updatedAt: DateTime.Utc) =>
        request({ type: "get_session_stats" }, 2_000).pipe(
          Effect.map((stats) => tokenUsageFromStats(stats, fallbackUsedTokens, updatedAt)),
          Effect.orElseSucceed(() => undefined),
        );

      /**
       * Draht attaches the current message's cumulative usage to every streaming update.
       */
      const reportLiveUsage =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (turn: ActiveDrahtTurn, usage: unknown) {
          const usedTokens = nonNegativeInteger(usage, "totalTokens");
          if (
            usedTokens === undefined ||
            usedTokens === 0 ||
            usedTokens === turn.lastLiveUsedTokens ||
            contextWindow === null ||
            contextWindow === 0
          ) {
            return;
          }
          turn.lastLiveUsedTokens = usedTokens;
          const inputTokens = nonNegativeInteger(usage, "input");
          const cachedInputTokens = nonNegativeInteger(usage, "cacheRead");
          const outputTokens = nonNegativeInteger(usage, "output");
          const updatedAt = yield* DateTime.now;
          yield* emit({
            type: "provider_turn.updated",
            driver: DRAHT_PROVIDER,
            threadId: turn.turnInput.threadId,
            providerTurn: {
              ...turn.providerTurn,
              tokenUsage: {
                usedTokens,
                maxTokens: contextWindow,
                ...(inputTokens === undefined ? {} : { inputTokens }),
                ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
                ...(outputTokens === undefined ? {} : { outputTokens }),
                updatedAt: DateTime.formatIso(updatedAt),
              },
            },
          });
        });

      const baseItemFields = (
        turn: ActiveDrahtTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({
          driver: DRAHT_PROVIDER,
          nativeItemId,
        }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({
          driver: DRAHT_PROVIDER,
          nativeItemId,
        }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActiveDrahtTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: DRAHT_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({
              driver: DRAHT_PROVIDER,
              nativeItemId,
            }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      const emitProviderRetry = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        providerRetry: DrahtProviderRetryState,
        status: "running" | "completed" | "failed" | "interrupted" | "cancelled",
        updatedAt: DateTime.Utc,
      ) {
        yield* emit({
          type: "turn_item.updated",
          driver: DRAHT_PROVIDER,
          turnItem: makeProviderRetryTurnItem({
            driver: DRAHT_PROVIDER,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            itemOrdinal: providerRetry.itemOrdinal,
            failure: providerRetry.failure,
            retry: providerRetry.retry,
            status,
            startedAt: providerRetry.startedAt,
            updatedAt,
          }),
        });
      });

      const compactionNativeItemId = (turn: ActiveDrahtTurn): string =>
        `compaction:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;

      const emitCompaction = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        compaction: DrahtCompactionState,
        status: DrahtCompactionStatus,
        details: {
          readonly summary?: string;
          readonly beforeTokenCount?: number;
          readonly afterTokenCount?: number;
        } = {},
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(
          turn,
          compaction.nativeItemId,
          "system",
          status,
          compaction.startedAt,
          completedAt,
        );
        yield* emit({
          type: "turn_item.updated",
          driver: DRAHT_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, compaction.nativeItemId, compaction.startedAt, emittedAt),
            status,
            title: compactionTitle(status),
            completedAt,
            type: "compaction",
            driver: DRAHT_PROVIDER,
            ...details,
          },
        });
      });

      const emitStreamItem =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (
          turn: ActiveDrahtTurn,
          item: DrahtStreamItemState,
          streaming: boolean,
        ) {
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            streaming ? "running" : "completed",
            item.startedAt,
            streaming ? null : emittedAt,
          );
          if (item.kind === "assistant_message") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver: DRAHT_PROVIDER,
              nativeItemId: item.nativeItemId,
            });
            yield* emit({
              type: "turn_item.updated",
              driver: DRAHT_PROVIDER,
              turnItem: {
                ...base,
                status: streaming ? "running" : "completed",
                title: null,
                completedAt: streaming ? null : emittedAt,
                type: "assistant_message",
                messageId,
                text: item.text,
                streaming,
              },
            });
            yield* emit({
              type: "message.updated",
              driver: DRAHT_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: idAllocator.derive.nodeFromProviderItem({
                  driver: DRAHT_PROVIDER,
                  nativeItemId: item.nativeItemId,
                }),
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: emittedAt,
              },
            });
            return;
          }
          yield* emit({
            type: "turn_item.updated",
            driver: DRAHT_PROVIDER,
            turnItem: {
              ...base,
              status: streaming ? "running" : "completed",
              title: null,
              completedAt: streaming ? null : emittedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
        });

      const scheduleStreamFlush =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (turn: ActiveDrahtTurn, item: DrahtStreamItemState) {
          if (item.flushScheduled || item.completed) return;
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                item.flushScheduled = false;
                return item.completed ? Effect.void : emitStreamItem(turn, item, true);
              }),
            ),
            Effect.forkIn(scope),
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        kind: DrahtStreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = `${turn.providerTurn.id}:m${turn.messageOrdinal}:c${contentIndex}`;
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const startedAt = yield* DateTime.now;
        const item: DrahtStreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt,
        };
        turn.streamItems.set(nativeItemId, item);
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const completeStreamItem = (
        turn: ActiveDrahtTurn,
        item: DrahtStreamItemState,
        text?: string,
      ) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          if (text !== undefined && text.length > 0) item.text = text;
          return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const completeOpenStreamItems = (turn: ActiveDrahtTurn) =>
        Effect.forEach(
          Array.from(turn.streamItems.values()).filter((item) => !item.completed),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );

      const emitToolItem = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        event: DrahtRpcRecord,
        phase: "start" | "update" | "end",
      ) {
        const toolCallId = recordString(event, "toolCallId");
        const toolName = recordString(event, "toolName") ?? "tool";
        if (toolCallId === undefined) return;
        if (phase === "start") {
          turn.toolArgs.set(toolCallId, event["args"]);
        }
        const args = event["args"] ?? turn.toolArgs.get(toolCallId);
        const emittedAt = yield* DateTime.now;
        const startedAt = turn.toolStartedAt.get(toolCallId) ?? emittedAt;
        turn.toolStartedAt.set(toolCallId, startedAt);
        const completed = phase === "end";
        const isError = event["isError"] === true;
        const resultRecord = completed ? event["result"] : event["partialResult"];
        const outputText = contentText(recordField(resultRecord, "content"));
        const status = completed
          ? isError
            ? turn.interrupted
              ? "interrupted"
              : "failed"
            : "completed"
          : "running";
        const base = baseItemFields(turn, toolCallId, startedAt, emittedAt);
        yield* emitItemNode(
          turn,
          toolCallId,
          "tool_call",
          status,
          startedAt,
          completed ? emittedAt : null,
        );
        const shared = {
          ...base,
          status,
          completedAt: completed ? emittedAt : null,
        } as const;
        if (toolName === "bash" || toolName === "powershell") {
          const exitCode =
            recordNumber(recordField(resultRecord, "structuredContent"), "exit_code") ??
            recordNumber(recordField(resultRecord, "details"), "exitCode");
          yield* emit({
            type: "turn_item.updated",
            driver: DRAHT_PROVIDER,
            turnItem: {
              ...shared,
              title: toolName,
              type: "command_execution",
              input: recordString(args, "command") ?? "",
              ...(outputText.length > 0 ? { output: outputText } : {}),
              ...(exitCode === undefined ? {} : { exitCode }),
            },
          });
          return;
        }
        if (toolName === "edit" || toolName === "write") {
          const fileName = recordString(args, "path") ?? recordString(args, "file_path");
          if (fileName !== undefined) {
            const diffStr =
              recordString(recordField(resultRecord, "details"), "patch") ??
              (isError && outputText.trim().length > 0 ? outputText : undefined);
            const newStr = toolName === "write" ? recordString(args, "content") : undefined;
            yield* emit({
              type: "turn_item.updated",
              driver: DRAHT_PROVIDER,
              turnItem: {
                ...shared,
                title: toolName,
                type: "file_change",
                fileName,
                ...(diffStr === undefined ? {} : { diffStr }),
                ...(newStr === undefined ? {} : { newStr }),
              },
            });
            return;
          }
        }
        const output = drahtToolOutput(resultRecord, toolName);
        yield* emit({
          type: "turn_item.updated",
          driver: DRAHT_PROVIDER,
          turnItem: {
            ...shared,
            title: toolName,
            type: "dynamic_tool",
            ...mcpToolPresentation({ toolName }),
            toolName,
            input: args ?? {},
            ...(output === undefined ? {} : { output }),
          },
        });
        if (toolName === DRAHT_SUBAGENT_TOOL || toolName === DRAHT_WORKFLOW_TOOL) {
          yield* emitPolyphaseRun(turn, toolCallId, resultRecord, status);
        } else if (toolName === DRAHT_DUET_DELEGATE_TOOL) {
          yield* emitDuetDelegation(turn, toolCallId, args, resultRecord, outputText, status);
        }
      });

      /**
       * A subagent's identity is fixed by the turn that first reported it. Background workflows
       * report again after that turn ends, so records live for the session, not the turn.
       */
      interface DrahtSubagentRecord {
        readonly base: ReturnType<typeof baseItemFields>;
        readonly rootNodeId: NodeId;
        readonly parentNodeId: NodeId;
        /** Agent labels whose child permission asks belong to this subagent. */
        readonly labels: Set<string>;
        readonly toolCallId: string | null;
        readonly background: boolean;
        lastUpdate: DrahtSubagentUpdate | null;
        signature: string | null;
      }

      const subagentRecords = new Map<string, DrahtSubagentRecord>();
      const backgroundWorkflows: Array<{ readonly name: string; readonly nativeTaskId: string }> =
        [];

      const isSettledRecord = (record: DrahtSubagentRecord): boolean =>
        record.lastUpdate !== null && isTerminalDrahtSubagentStatus(record.lastUpdate.status);

      const openSubagent = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        nativeTaskId: string,
        open: {
          readonly parentNodeId: NodeId;
          readonly toolCallId: string | null;
          readonly background: boolean;
          readonly startedAt: number | undefined;
        },
      ) {
        const existing = subagentRecords.get(nativeTaskId);
        if (existing !== undefined) return existing;
        const emittedAt = yield* DateTime.now;
        const startedAt =
          open.startedAt === undefined
            ? emittedAt
            : Option.getOrElse(DateTime.make(open.startedAt), () => emittedAt);
        const record: DrahtSubagentRecord = {
          base: baseItemFields(turn, nativeTaskId, startedAt, emittedAt),
          rootNodeId: turn.turnInput.rootNodeId,
          parentNodeId: open.parentNodeId,
          labels: new Set(),
          toolCallId: open.toolCallId,
          background: open.background,
          lastUpdate: null,
          signature: null,
        };
        subagentRecords.set(nativeTaskId, record);
        return record;
      });

      const reportSubagent =
        /* Polyphase reports arrive with every throttled tool update; avoid a span for each. */
        Effect.fnUntraced(function* (record: DrahtSubagentRecord, update: DrahtSubagentUpdate) {
          if (isSettledRecord(record)) return;
          const signature = encodeJson(update);
          if (signature === record.signature) return;
          record.signature = signature;
          record.lastUpdate = update;
          const emittedAt = yield* DateTime.now;
          const settled = isTerminalDrahtSubagentStatus(update.status);
          const completedAt = !settled
            ? null
            : update.endedAt === undefined
              ? emittedAt
              : Option.getOrElse(DateTime.make(update.endedAt), () => emittedAt);
          const { base } = record;
          const progress =
            settled || update.progress === undefined ? {} : { progress: update.progress };
          yield* emit({
            type: "subagent.updated",
            driver: DRAHT_PROVIDER,
            subagent: {
              id: base.nodeId,
              threadId: base.threadId,
              runId: base.runId,
              parentNodeId: record.parentNodeId,
              origin: "provider_native",
              createdBy: "agent",
              driver: DRAHT_PROVIDER,
              providerInstanceId: options.instanceId,
              providerThreadId: base.providerThreadId,
              childThreadId: null,
              nativeTaskRef: base.nativeItemRef,
              prompt: update.prompt,
              title: update.title,
              model: update.model,
              status: update.status,
              ...progress,
              ...(update.workflow === undefined ? {} : { workflow: update.workflow }),
              result: update.result,
              startedAt: base.startedAt,
              completedAt,
              updatedAt: emittedAt,
            },
          });
          yield* emit({
            type: "node.updated",
            driver: DRAHT_PROVIDER,
            node: {
              id: base.nodeId,
              threadId: base.threadId,
              runId: base.runId,
              parentNodeId: record.parentNodeId,
              rootNodeId: record.rootNodeId,
              kind: "subagent",
              status: update.status,
              countsForRun: false,
              providerThreadId: base.providerThreadId,
              providerTurnId: base.providerTurnId,
              nativeItemRef: base.nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: base.startedAt,
              completedAt,
            },
          });
          yield* emit({
            type: "turn_item.updated",
            driver: DRAHT_PROVIDER,
            turnItem: {
              ...base,
              updatedAt: emittedAt,
              status: update.status,
              title: update.title,
              completedAt,
              type: "subagent",
              subagentId: base.nodeId,
              origin: "provider_native",
              driver: DRAHT_PROVIDER,
              providerInstanceId: options.instanceId,
              childThreadId: null,
              prompt: update.prompt,
              ...progress,
              result: update.result,
            },
          });
        });

      /** Re-sends each open matching subagent's last report with a terminal status. */
      const settleSubagents = Effect.fnUntraced(function* (
        matches: (record: DrahtSubagentRecord) => boolean,
        status: DrahtTerminalSubagentStatus,
      ) {
        for (const record of subagentRecords.values()) {
          if (!matches(record) || record.lastUpdate === null || isSettledRecord(record)) continue;
          yield* reportSubagent(record, { ...record.lastUpdate, status, progress: undefined });
        }
      });

      const forgetSettledSubagents = () => {
        for (const [nativeTaskId, record] of subagentRecords) {
          if (isSettledRecord(record)) subagentRecords.delete(nativeTaskId);
        }
      };

      /**
       * A `workflow` run is one subagent whose members are its agents; a `subagent` run reports
       * each agent as its own subagent, as its single, parallel, and chain modes run independent
       * tasks.
       */
      const emitPolyphaseRun = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        toolCallId: string,
        resultRecord: unknown,
        toolStatus: "running" | "completed" | "failed" | "interrupted",
      ) {
        const details = parseDrahtPolyphaseDetails(recordField(resultRecord, "details"));
        if (details === null) {
          // A tool call that failed before the run started ends without polyphase details.
          if (toolStatus !== "running") {
            yield* settleSubagents((record) => record.toolCallId === toolCallId, toolStatus);
          }
          return;
        }
        const parentNodeId = idAllocator.derive.nodeFromProviderItem({
          driver: DRAHT_PROVIDER,
          nativeItemId: toolCallId,
        });
        const reportedStatus = drahtRunStatus(details.status);
        // Final details always settle the run; a stopped or failed call can end without them.
        const runStatus =
          toolStatus === "running" || isTerminalDrahtSubagentStatus(reportedStatus)
            ? reportedStatus
            : toolStatus;
        if (details.kind === "workflow") {
          const record = yield* openSubagent(turn, `${toolCallId}:workflow`, {
            parentNodeId,
            toolCallId,
            background: false,
            startedAt: details.startedAt,
          });
          for (const agent of details.agents) record.labels.add(agent.label);
          const settled = isTerminalDrahtSubagentStatus(runStatus);
          yield* reportSubagent(record, {
            title: details.title,
            prompt: drahtRunPrompt(details),
            model: null,
            status: runStatus,
            progress: drahtRunProgress(details),
            result: settled ? (drahtRunResult(details) ?? null) : null,
            workflow: drahtWorkflowProgress(details, record.lastUpdate?.workflow),
            endedAt: details.endedAt,
          });
          return;
        }
        for (const agent of details.agents) {
          const record = yield* openSubagent(turn, `${toolCallId}:agent:${agent.i}`, {
            parentNodeId,
            toolCallId,
            background: false,
            startedAt: agent.startedAt,
          });
          record.labels.add(agent.label);
          const reported = drahtAgentStatus(agent.status);
          const status =
            isTerminalDrahtSubagentStatus(runStatus) && !isTerminalDrahtSubagentStatus(reported)
              ? runStatus
              : reported;
          const settled = isTerminalDrahtSubagentStatus(status);
          yield* reportSubagent(record, {
            title: agent.label.trim() || agent.agent,
            prompt: agent.task,
            model: agent.model?.trim() || null,
            status,
            progress: drahtAgentProgress(agent),
            result: settled ? (drahtAgentResult(agent) ?? null) : null,
            workflow: undefined,
            endedAt: agent.endedAt,
          });
        }
        // A large run can leave settled agents out of its bounded final details.
        if (isTerminalDrahtSubagentStatus(runStatus)) {
          yield* settleSubagents((record) => record.toolCallId === toolCallId, runStatus);
        }
      });

      /**
       * Duet triage teammates run through the polyphase runner, but `duet_delegate` reports only
       * activity lines while they run and each teammate's exit code and output section at the end.
       */
      const emitDuetDelegation = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        toolCallId: string,
        args: unknown,
        resultRecord: unknown,
        outputText: string,
        toolStatus: "running" | "completed" | "failed" | "interrupted",
      ) {
        const assignments = parseDrahtDuetAssignments(args);
        if (assignments.length === 0) return;
        const parentNodeId = idAllocator.derive.nodeFromProviderItem({
          driver: DRAHT_PROVIDER,
          nativeItemId: toolCallId,
        });
        const toolEnded = toolStatus !== "running";
        const exitCodes = toolEnded
          ? parseDrahtDuetExitCodes(recordField(resultRecord, "details"))
          : new Map<string, number>();
        const sections = toolEnded ? parseDrahtDuetSections(outputText) : new Map<string, string>();
        const activity = toolEnded ? new Map<string, string>() : parseDrahtDuetActivity(outputText);
        for (const [index, assignment] of assignments.entries()) {
          const record = yield* openSubagent(turn, `${toolCallId}:duet:${index}`, {
            parentNodeId,
            toolCallId,
            background: false,
            startedAt: undefined,
          });
          record.labels.add(assignment.participant);
          const exitCode = exitCodes.get(assignment.participant);
          // Draht refuses a delegation before any teammate starts (duet off, a repeat call, an
          // unknown teammate) with an explanation and no exit codes.
          const status = !toolEnded
            ? "running"
            : exitCode !== undefined
              ? exitCode === 0
                ? "completed"
                : "failed"
              : toolStatus === "completed"
                ? "cancelled"
                : toolStatus;
          const result = toolEnded
            ? (sections.get(assignment.participant) ??
              (exitCode === undefined && outputText.trim().length > 0 ? outputText.trim() : null))
            : null;
          yield* reportSubagent(record, {
            title: assignment.participant,
            prompt: assignment.task,
            model:
              duet.participants.find((participant) => participant.role === assignment.participant)
                ?.modelRef ?? null,
            status,
            progress: activity.get(assignment.participant),
            result,
            workflow: undefined,
            endedAt: undefined,
          });
        }
      });

      const publishBackgroundWorkflows = (state: DrahtThreadState) =>
        updateProviderThread(state, {
          pendingBackgroundTasks: backgroundWorkflows.map(({ name, nativeTaskId }) => ({
            taskId: nativeTaskId,
            kind: "background_task" as const,
            description: `Workflow ${name}`,
          })),
        });

      /**
       * A `/workflow` command announces its run and returns, so the T3 turn settles while the run
       * continues inside Draht and reports only when it finishes.
       */
      const startBackgroundWorkflow = Effect.fnUntraced(function* (
        state: DrahtThreadState,
        turn: ActiveDrahtTurn,
        name: string,
      ) {
        const nativeTaskId = `${turn.providerTurn.id}:workflow:${name}:${turn.nextItemOrdinal}`;
        const record = yield* openSubagent(turn, nativeTaskId, {
          parentNodeId: turn.turnInput.rootNodeId,
          toolCallId: null,
          background: true,
          startedAt: undefined,
        });
        yield* reportSubagent(record, {
          title: `Workflow ${name}`,
          prompt: name,
          model: null,
          status: "running",
          progress: undefined,
          result: null,
          workflow: { name, phases: [], agents: [] },
          endedAt: undefined,
        });
        backgroundWorkflows.push({ name, nativeTaskId });
        yield* publishBackgroundWorkflows(state);
      });

      const finishBackgroundWorkflow = Effect.fnUntraced(function* (
        state: DrahtThreadState | null,
        turn: ActiveDrahtTurn | null,
        detailsValue: unknown,
      ) {
        const details = parseDrahtPolyphaseDetails(detailsValue);
        if (details === null) return;
        const name = details.workflow?.name;
        const index = backgroundWorkflows.findIndex((workflow) => workflow.name === name);
        const pending = index === -1 ? undefined : backgroundWorkflows[index];
        const record =
          pending === undefined
            ? turn === null
              ? undefined
              : yield* openSubagent(turn, `${turn.providerTurn.id}:workflow:${details.runId}`, {
                  parentNodeId: turn.turnInput.rootNodeId,
                  toolCallId: null,
                  background: true,
                  startedAt: details.startedAt,
                })
            : subagentRecords.get(pending.nativeTaskId);
        // Clear the roster first: once the run's last open item settles, the orchestrator asks
        // whether background work remains and stops reading this session's events.
        if (pending !== undefined) {
          backgroundWorkflows.splice(index, 1);
          if (state !== null) yield* publishBackgroundWorkflows(state);
        }
        if (record !== undefined) {
          const reportedStatus = drahtRunStatus(details.status);
          yield* reportSubagent(record, {
            title: details.title,
            prompt: drahtRunPrompt(details),
            model: null,
            status: isTerminalDrahtSubagentStatus(reportedStatus) ? reportedStatus : "completed",
            progress: undefined,
            result: drahtRunResult(details) ?? null,
            workflow: drahtWorkflowProgress(details, record.lastUpdate?.workflow),
            endedAt: details.endedAt,
          });
        }
        forgetSettledSubagents();
      });

      let duet: DrahtDuet = DRAHT_DUET_OFF;
      /** The participant last announced, so each turn names who answers it once. */
      let duetAnnouncement: { readonly providerTurnId: string; readonly role: string } | null =
        null;
      /** The turn that switched duet on, which no participant answers. */
      let duetEnablingTurnId: string | null = null;

      /**
       * Duet owns the model while it is on: it resets any other selection with a warning, so T3
       * stops applying its own and re-applies it once duet turns off. Draht announces duet
       * configuration changes itself; in turns strategy only its status line says who answers.
       */
      const updateDuet = Effect.fnUntraced(function* (next: DrahtDuet) {
        const previous = duet;
        duet = next;
        if (previous.enabled && !next.enabled) {
          appliedModel = null;
          appliedThinking = null;
        }
        const turn = threadState?.activeTurn ?? null;
        if (turn === null) return;
        if (!previous.enabled && next.enabled) duetEnablingTurnId = turn.providerTurn.id;
        const role = next.role;
        if (
          !next.enabled ||
          next.strategy !== "turns" ||
          role === undefined ||
          duetEnablingTurnId === turn.providerTurn.id ||
          (duetAnnouncement?.providerTurnId === turn.providerTurn.id &&
            duetAnnouncement.role === role)
        ) {
          return;
        }
        duetAnnouncement = { providerTurnId: turn.providerTurn.id, role };
        const participant = next.participants.find((candidate) => candidate.role === role);
        yield* emitSystemNotice(
          turn,
          `Duet turn: ${participant === undefined ? role : formatDrahtDuetParticipant(participant)}`,
        );
      });

      const emitSystemNotice = Effect.fnUntraced(function* (
        turn: ActiveDrahtTurn,
        message: string,
      ) {
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `notify:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
        yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: DRAHT_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "completed",
            completedAt: emittedAt,
            title: message,
            type: "system_notice",
            message,
          },
        });
      });

      const cancelPrompt = Effect.fn("DrahtAdapter.cancelPrompt")(function* (
        pending: PendingDrahtPrompt,
        resolvedAt: DateTime.Utc,
      ) {
        if (pending.expirationFiber !== null) yield* Fiber.interrupt(pending.expirationFiber);
        yield* connection
          .send({
            type: "extension_ui_response",
            id: pending.nativeRequestId,
            cancelled: true,
          })
          .pipe(Effect.ignore);
        pending.runtimeRequest = {
          ...pending.runtimeRequest,
          status: "cancelled",
          resolvedAt,
        };
        yield* emit({
          type: "runtime_request.updated",
          driver: DRAHT_PROVIDER,
          threadId: pending.node.threadId,
          runtimeRequest: pending.runtimeRequest,
        });
        yield* emit({
          type: "node.updated",
          driver: DRAHT_PROVIDER,
          node: { ...pending.node, status: "cancelled", completedAt: resolvedAt },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DRAHT_PROVIDER,
          turnItem: {
            ...pending.turnItem,
            status: "cancelled",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          },
        });
      });

      const cancelPendingPrompts = Effect.fn("DrahtAdapter.cancelPendingPrompts")(function* (
        resolvedAt: DateTime.Utc,
      ) {
        const pending = Array.from(pendingPrompts.values());
        pendingPrompts.clear();
        yield* Effect.forEach(pending, (prompt) => cancelPrompt(prompt, resolvedAt), {
          discard: true,
        });
      });

      const handleExtensionUiRequest = Effect.fnUntraced(function* (event: DrahtRpcRecord) {
        const method = recordString(event, "method");
        const nativeRequestId = recordString(event, "id");
        if (method === undefined) return;
        if (method === "notify") {
          const state = threadState;
          const turn = state?.activeTurn ?? null;
          const message = recordString(event, "message") ?? "";
          if (state === null || turn === null || message.length === 0) return;
          yield* emitSystemNotice(turn, message);
          const workflow = parseDrahtStartedWorkflow(message);
          if (workflow !== null) yield* startBackgroundWorkflow(state, turn, workflow);
          return;
        }
        if (method === "setStatus") {
          if (recordString(event, "statusKey") !== DRAHT_DUET_STATUS_KEY) return;
          const status = parseDrahtDuetStatus(recordString(event, "statusText"));
          yield* updateDuet({
            enabled: status !== null,
            strategy: status?.strategy ?? duet.strategy,
            role: status?.role,
            participants: duet.participants,
          });
          return;
        }
        if (
          method !== "select" &&
          method !== "confirm" &&
          method !== "input" &&
          method !== "editor"
        ) {
          yield* Effect.logDebug("Ignoring draht extension UI update.", { method });
          return;
        }
        if (nativeRequestId === undefined) return;
        const approvalTitle = recordString(event, "title") ?? "";
        const detail = recordField(event, "detail");
        // Draht's gate keeps the gated command and path in `detail`, not in its generic title and
        // message, so an approval remembered for the session must match on them too.
        const approvalKey = encodeJson([
          approvalTitle,
          recordString(event, "message") ?? "",
          ...["toolName", "command", "path", "operation", "cwd"].map((key) =>
            recordString(detail, key),
          ),
        ]);
        if (method === "confirm" && sessionApprovals.has(approvalKey)) {
          yield* connection.send({
            type: "extension_ui_response",
            id: nativeRequestId,
            confirmed: true,
          });
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const createdAt = yield* DateTime.now;
        const timeout = recordNumber(event, "timeout");
        // Draht treats zero as no timeout and negative durations as immediate expiry.
        const expiresAt =
          timeout === undefined || timeout === 0
            ? null
            : DateTime.toEpochMillis(createdAt) + Math.max(0, timeout);
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: DRAHT_PROVIDER,
          ...(turn === null ? {} : { providerTurnId: turn.providerTurn.id }),
          nativeRequestId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const title = recordString(event, "title") ?? method;
        // Polyphase children forward their asks through this session with no run id, so they
        // attach to the subagent only when exactly one open subagent has the asking label.
        const childLabel = method === "confirm" ? drahtChildApprovalLabel(detail) : undefined;
        const childOwners =
          childLabel === undefined
            ? []
            : Array.from(subagentRecords.values()).filter(
                (record) => !isSettledRecord(record) && record.labels.has(childLabel),
              );
        const childOwner = childOwners.length === 1 ? childOwners[0] : undefined;
        const optionIds = method === "confirm" ? drahtApprovalOptionIds(detail) : {};
        const threadId =
          turn?.turnInput.threadId ?? state?.providerThread.appThreadId ?? input.threadId;
        const providerThreadId = state?.providerThread.id ?? null;
        const providerTurnId = turn?.providerTurn.id ?? null;
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId,
          nativeRequestRef: providerRef(nativeRequestId),
          kind: method === "confirm" ? "command" : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId,
          runId: turn?.turnInput.runId ?? null,
          parentNodeId: childOwner?.base.nodeId ?? turn?.turnInput.rootNodeId ?? null,
          rootNodeId: turn?.turnInput.rootNodeId ?? nodeId,
          kind: method === "confirm" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: createdAt,
          completedAt: null,
        };
        const itemBase = {
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          threadId,
          runId: turn?.turnInput.runId ?? null,
          nodeId,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          parentItemId: null,
          ordinal: turn === null ? 0 : itemOrdinal(turn, nativeRequestId),
          status: "waiting" as const,
          title,
          startedAt: createdAt,
          completedAt: null,
          updatedAt: createdAt,
        };
        const turnItem: OrchestrationV2TurnItem =
          method === "confirm"
            ? {
                ...itemBase,
                type: "approval_request",
                requestId,
                requestKind: drahtApprovalRequestKind(event, title),
                prompt: recordString(event, "message") ?? title,
                ...drahtApprovalOptions(detail),
              }
            : {
                ...itemBase,
                type: "user_input_request",
                requestId,
                questions: [drahtQuestion(nativeRequestId, method, title, event)],
              };
        const pending: PendingDrahtPrompt = {
          nativeRequestId,
          method,
          questionId: nativeRequestId,
          approvalKey,
          optionIds,
          expiresAt,
          expirationFiber: null,
          runtimeRequest,
          node,
          turnItem,
        };
        pendingPrompts.set(String(requestId), pending);
        yield* emit({
          type: "runtime_request.updated",
          driver: DRAHT_PROVIDER,
          threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: DRAHT_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: DRAHT_PROVIDER, turnItem });
        if (expiresAt !== null) {
          const now = yield* DateTime.now;
          pending.expirationFiber = yield* Effect.sleep(
            Duration.millis(Math.max(0, expiresAt - DateTime.toEpochMillis(now))),
          ).pipe(
            Effect.andThen(
              Queue.offer(connection.events, {
                type: "t3.ui_request_expired",
                requestId: String(requestId),
                pending,
              }),
            ),
            Effect.asVoid,
            Effect.forkIn(scope),
          );
        }
      });

      const emitExtensionError = Effect.fnUntraced(function* (event: DrahtRpcRecord) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (turn === null) {
          outOfTurnExtensionErrors.push(event);
          return;
        }
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `extension-error:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
        const extensionName = drahtExtensionDisplayName(recordString(event, "extensionPath"));
        const extensionEvent = recordString(event, "event");
        const detail = recordString(event, "error")?.trim();
        const message = [
          `${extensionName} failed${extensionEvent === undefined ? "" : ` during ${extensionEvent}`}.`,
          detail === undefined || detail.length === 0 ? undefined : detail.slice(0, 2_000),
        ]
          .filter((part): part is string => part !== undefined)
          .join("\n\n");
        const failure = makeProviderFailure({
          message,
          class: "provider_error",
          retryable: false,
        });
        yield* emitItemNode(turn, nativeItemId, "system", "failed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: DRAHT_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "failed",
            title: extensionName,
            completedAt: emittedAt,
            type: "error",
            failure,
          },
        });
      });

      const listSessionEntries = (since: string | null, timeoutMs: number) =>
        request({ type: "get_entries", ...(since === null ? {} : { since }) }, timeoutMs).pipe(
          Effect.map(drahtSessionListing),
          Effect.orElseSucceed(() => null),
        );

      /**
       * `get_entries` is append-ordered and keeps abandoned branches, so both come from walking
       * the leaf's `parentId` chain, never from list order.
       */
      const captureTurnTreeRefs = Effect.fnUntraced(function* (
        timeoutMs = DRAHT_REQUEST_TIMEOUT_MS,
      ): Effect.fn.Return<DrahtTurnTreeRefs | null> {
        const cursorWasStale = leafCursorStale;
        const previousLeaf = lastKnownLeaf;
        const fullListing = cursorWasStale || previousLeaf === null;
        const listing = yield* listSessionEntries(fullListing ? null : previousLeaf, timeoutMs);
        const turnWalk =
          listing === null ? null : walkDrahtBranch(listing.entries, listing.leafId, previousLeaf);
        const rewound = turnWalk !== null && previousLeaf !== null && !turnWalk.reachedStop;
        // A `since` window ends at the previous leaf; the rewound branch
        // continues past it, so its ancestry needs the whole tree.
        const tree = rewound && !fullListing ? yield* listSessionEntries(null, timeoutMs) : listing;
        if (listing === null || turnWalk === null || tree === null) {
          leafCursorStale = true;
          return null;
        }
        lastKnownLeaf = listing.leafId;
        leafCursorStale = false;
        const turnStartEntryId = turnWalk.userEntryIds.at(-1);
        // Only a branch walked to its root lists every survivor; a partial or
        // cyclic walk would roll back runs that are still in the conversation.
        // A replaced session has a different tree. Only an old leaf still in
        // this tree proves an in-session rewind rather than a session switch.
        const branch =
          rewound && previousLeaf !== null && tree.entries.has(previousLeaf)
            ? walkDrahtBranch(tree.entries, listing.leafId, null)
            : undefined;
        return {
          // Without a trustworthy cursor the walk spans more than one turn, so its earliest user
          // entry belongs to an earlier turn. Keep the synthetic ref rather than pointing
          // rollback too far back; the next turn gets an accurate ref again.
          nativeTurnRef: cursorWasStale
            ? undefined
            : turnStartEntryId !== undefined
              ? providerRef(turnStartEntryId)
              : turnWalk.entryCount === 0
                ? null
                : undefined,
          leafId: listing.leafId,
          retainedNativeTurnIds: branch?.reachedStop === true ? branch.userEntryIds : undefined,
        };
      });

      const finalizeTurn = Effect.fnUntraced(function* (state: DrahtThreadState, readUsage = true) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const interrupted = turn.interrupted || turn.nativeAborted;
        const completedAt = yield* DateTime.now;
        yield* completeOpenStreamItems(turn);
        if (turn.activeCompaction !== null) {
          const status = interrupted ? "cancelled" : turn.failure === null ? "completed" : "failed";
          yield* emitCompaction(turn, turn.activeCompaction, status);
          turn.activeCompaction = null;
        }
        if (turn.activeProviderRetry !== null) {
          if (interrupted) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "interrupted", completedAt);
            turn.activeProviderRetry = null;
          } else if (turn.failure === null) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "completed", completedAt);
            turn.activeProviderRetry = null;
          }
        }
        yield* cancelPendingPrompts(completedAt);
        // Tool runs end with their tool call. Background workflows keep running past the turn.
        yield* settleSubagents(
          (record) => !record.background && record.base.providerTurnId === turn.providerTurn.id,
          interrupted
            ? "interrupted"
            : (turn.rejectedPromptFailure ?? turn.failure) === null
              ? "completed"
              : "failed",
        );
        forgetSettledSubagents();
        const treeRefs =
          turn.stopTreeRefs !== undefined ? turn.stopTreeRefs : yield* captureTurnTreeRefs();
        const tokenUsage = readUsage
          ? yield* readTokenUsage(turn.latestCompactionAfterTokens, completedAt)
          : undefined;
        const failure = interrupted ? null : (turn.rejectedPromptFailure ?? turn.failure);
        yield* emit({
          type: "provider_turn.updated",
          driver: DRAHT_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            ...(treeRefs === null || treeRefs.nativeTurnRef === undefined
              ? {}
              : { nativeTurnRef: treeRefs.nativeTurnRef }),
            status: interrupted ? "interrupted" : failure !== null ? "failed" : "completed",
            completedAt,
            ...(tokenUsage === undefined ? {} : { tokenUsage }),
          },
        });
        yield* updateProviderThread(
          state,
          {
            status: "idle",
            ...(treeRefs === null
              ? {}
              : {
                  nativeConversationHeadRef:
                    treeRefs.leafId === null ? null : providerRef(treeRefs.leafId),
                }),
          },
          treeRefs?.retainedNativeTurnIds,
        );
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          if (turn.activeProviderRetry !== null) {
            yield* emitProviderRetry(
              turn,
              { ...turn.activeProviderRetry, failure },
              "failed",
              completedAt,
            );
          } else {
            yield* emit({
              type: "turn_item.updated",
              driver: DRAHT_PROVIDER,
              turnItem: {
                ...baseItemFields(turn, failureItemId, completedAt, completedAt),
                status: "failed",
                title: null,
                completedAt,
                type: "error",
                failure,
              },
            });
          }
          yield* emit({
            type: "turn.terminal",
            driver: DRAHT_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure,
            ...(turn.activeProviderRetry === null
              ? {}
              : {
                  retry: turn.activeProviderRetry.retry,
                  retryStartedAt: turn.activeProviderRetry.startedAt,
                }),
            threadDisposition: "reusable",
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: DRAHT_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            status: interrupted ? "interrupted" : "completed",
            failure: null,
            threadDisposition: "reusable",
          });
        }
      });

      const scheduleSettleProbe = (
        turn: ActiveDrahtTurn,
        settleAfterAgentActivity = false,
        attempt = 1,
      ) => {
        const providerTurnId = turn.providerTurn.id;
        const settleProbeGeneration = turn.settleProbeGeneration;
        return request({ type: "get_state" }, 2_000).pipe(
          Effect.matchEffect({
            onSuccess: (data) =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                data,
              }),
            // A failed probe still has to reach the pump. Dropping it would
            // leave a command-only turn active forever, because Draht never emits
            // agent events for one.
            onFailure: () =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                probeFailed: true,
              }),
          }),
          Effect.ignore,
          Effect.forkIn(scope),
        );
      };

      const stopPendingWake = Effect.fnUntraced(function* (wake: DrahtWake) {
        if (pendingWake !== wake || threadState !== wake.state || closed) return;
        pendingWake = null;
        stopRequested = true;
        yield* cancelPendingPrompts(yield* DateTime.now);
        yield* connection.terminate;
      });

      const handleSessionEvent = Effect.fnUntraced(function* (event: DrahtRpcRecord) {
        // Model-selection hooks can await a dialog before startTurn can claim
        // a wake. Keep interactive requests session-scoped until a turn owns
        // them; buffer native output and notifications for that turn.
        const interactiveUiRequest =
          event["type"] === "extension_ui_request" &&
          ["select", "confirm", "input", "editor"].includes(String(event["method"]));
        if (
          threadState?.activeTurn === null &&
          pendingWake !== null &&
          !interactiveUiRequest &&
          !String(event["type"]).startsWith("t3.") &&
          event["type"] !== "response"
        ) {
          pendingWake.events.push(event);
          if (event["type"] === "agent_start") yield* updateProviderSession("running", null);
          if (event["type"] === "agent_settled") yield* updateProviderSession("ready", null);
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        switch (event["type"]) {
          case "agent_start": {
            if (
              turn === null &&
              state?.providerThread.appThreadId != null &&
              continuationRequests !== undefined &&
              !closed &&
              !stopRequested
            ) {
              const wake: DrahtWake = { state, events: [event] };
              pendingWake = wake;
              if (rollbackBarrier !== null) {
                yield* stopPendingWake(wake);
                return;
              }
              yield* updateProviderSession("running", null);
              yield* continuationRequests
                .offer({
                  threadId: state.providerThread.appThreadId,
                  providerThreadId: state.providerThread.id,
                  driver: DRAHT_PROVIDER,
                  detail: null,
                  delivery: "adapter_buffered",
                  dispatchIfCurrent: (dispatch) =>
                    sessionEventPermit.withPermits(1)(
                      Effect.suspend(() =>
                        pendingWake === wake && threadState === state && !closed
                          ? dispatch.pipe(
                              Effect.map(Option.some),
                              Effect.tapError(() => stopPendingWake(wake)),
                            )
                          : Effect.succeed(Option.none()),
                      ),
                    ),
                  clearIfCurrent: () => sessionEventPermit.withPermits(1)(stopPendingWake(wake)),
                })
                .pipe(Effect.tapError(() => stopPendingWake(wake)));
              return;
            }
            if (turn === null) {
              unsolicitedActivityDetected = true;
              yield* updateProviderSession("error", DRAHT_UNSOLICITED_ACTIVITY_ERROR);
              yield* connection.terminate;
              return;
            }
            turn.sawAgentActivity = true;
            turn.nativeAborted = false;
            turn.settleProbeGeneration += 1;
            return;
          }
          case "message_start": {
            if (turn !== null && recordString(event["message"], "role") === "assistant") {
              turn.sawAgentActivity = true;
              turn.messageOrdinal += 1;
            }
            return;
          }
          case "message_update": {
            if (turn === null) return;
            turn.sawAgentActivity = true;
            yield* reportLiveUsage(turn, event["usage"]);
            const delta = event["assistantMessageEvent"];
            const deltaType = recordString(delta, "type");
            const contentIndex = recordNumber(delta, "contentIndex") ?? 0;
            if (deltaType === "text_delta" || deltaType === "thinking_delta") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_delta" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              item.text += recordString(delta, "delta") ?? "";
              yield* scheduleStreamFlush(turn, item);
              return;
            }
            if (deltaType === "text_end" || deltaType === "thinking_end") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_end" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              yield* completeStreamItem(
                turn,
                item,
                recordString(delta, "content") ?? recordString(delta, "thinking"),
              );
              return;
            }
            return;
          }
          case "message_end": {
            const message = event["message"];
            if (recordString(message, "role") === "custom") {
              if (recordString(message, "customType") === DRAHT_WORKFLOW_MESSAGE) {
                yield* finishBackgroundWorkflow(state, turn, recordField(message, "details"));
              }
              return;
            }
            if (turn === null) return;
            if (recordString(message, "role") !== "assistant") return;
            yield* completeOpenStreamItems(turn);
            if (recordString(message, "stopReason") === "error" && turn.failure === null) {
              turn.failure = makeProviderFailure({
                message: recordString(message, "errorMessage") ?? "Draht reported a model error.",
                class: "provider_error",
              });
            }
            return;
          }
          case "tool_execution_start":
            if (turn !== null) {
              turn.sawAgentActivity = true;
              yield* emitToolItem(turn, event, "start");
            }
            return;
          case "tool_execution_update":
            if (turn !== null) yield* emitToolItem(turn, event, "update");
            return;
          case "tool_execution_end":
            if (turn !== null) yield* emitToolItem(turn, event, "end");
            return;
          case "compaction_start": {
            if (turn === null) return;
            turn.settleProbeGeneration += 1;
            turn.sawCompaction = true;
            if (turn.activeCompaction !== null) {
              yield* emitCompaction(turn, turn.activeCompaction, "cancelled");
            }
            const startedAt = yield* DateTime.now;
            const compaction = {
              nativeItemId: compactionNativeItemId(turn),
              startedAt,
            } satisfies DrahtCompactionState;
            turn.activeCompaction = compaction;
            yield* emitCompaction(turn, compaction, "running");
            return;
          }
          case "compaction_end": {
            if (turn === null) return;
            const observedAt = yield* DateTime.now;
            const compaction = turn.activeCompaction ?? {
              nativeItemId: compactionNativeItemId(turn),
              startedAt: observedAt,
            };
            turn.activeCompaction = null;
            const result = event["result"];
            if (result === null || result === undefined) {
              if (event["aborted"] === true) {
                yield* emitCompaction(turn, compaction, "cancelled");
                if (turn.settleWhenIdle || !turn.sawAgentActivity) {
                  yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
                }
                return;
              }
              const errorMessage =
                recordString(event, "errorMessage") ?? "Draht context compaction failed.";
              yield* emitCompaction(turn, compaction, "failed", {
                summary: errorMessage.slice(0, 1_000),
              });
              if (turn.settleWhenIdle || !turn.sawAgentActivity) {
                yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
              }
              return;
            }
            // An overflow can surface as a model error (`message_end` with
            // stopReason error) before Draht compacts and retries the turn. Clear
            // that failure only when Draht confirms that compaction will retry;
            // a successful non-retrying compaction must not erase an exhausted
            // provider retry.
            if (event["willRetry"] === true) turn.failure = null;
            turn.latestCompactionAfterTokens =
              nonNegativeInteger(result, "estimatedTokensAfter") ?? null;
            const summary = recordString(result, "summary");
            const beforeTokenCount = nonNegativeInteger(result, "tokensBefore");
            const afterTokenCount = nonNegativeInteger(result, "estimatedTokensAfter");
            yield* emitCompaction(turn, compaction, "completed", {
              ...(summary === undefined ? {} : { summary }),
              ...(beforeTokenCount === undefined ? {} : { beforeTokenCount }),
              ...(afterTokenCount === undefined ? {} : { afterTokenCount }),
            });
            if (turn.settleWhenIdle || !turn.sawAgentActivity) {
              yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
            }
            return;
          }
          case "auto_retry_start": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const maxAttempts = Math.max(
              attempt,
              Math.trunc(recordNumber(event, "maxAttempts") ?? attempt),
            );
            const retryDelayMs = Math.max(0, Math.trunc(recordNumber(event, "delayMs") ?? 0));
            const failure = makeProviderFailure({
              message: recordString(event, "errorMessage") ?? "Draht provider request failed.",
              class: "provider_error",
              retryable: true,
            });
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: { attempt, maxAttempts, retryDelayMs },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies DrahtProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            yield* emitProviderRetry(turn, providerRetry, "running", emittedAt);
            return;
          }
          case "auto_retry_end": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            if (event["success"] === true) {
              // before retrying, so leaving that failure in place would make
              // `agent_settled` terminalize a successful turn as failed.
              if (turn.activeProviderRetry !== null) {
                const attempt = Math.max(
                  1,
                  Math.trunc(
                    recordNumber(event, "attempt") ?? turn.activeProviderRetry.retry.attempt,
                  ),
                );
                const recoveredRetry = {
                  ...turn.activeProviderRetry,
                  retry: { ...turn.activeProviderRetry.retry, attempt },
                };
                yield* emitProviderRetry(turn, recoveredRetry, "completed", emittedAt);
                turn.activeProviderRetry = null;
              }
              turn.failure = null;
              return;
            }
            const failure = makeProviderFailure({
              message: recordString(event, "finalError") ?? "Draht auto-retry failed.",
              class: "provider_error",
              retryable: false,
            });
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: {
                attempt,
                maxAttempts: current?.retry.maxAttempts ?? attempt,
                retryDelayMs: current?.retry.retryDelayMs ?? null,
              },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies DrahtProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            turn.failure = failure;
            yield* emitProviderRetry(turn, providerRetry, "failed", emittedAt);
            return;
          }
          case "extension_ui_request":
            yield* handleExtensionUiRequest(event);
            return;
          case "entry_appended": {
            const duetState = parseDrahtDuetStateEntry(event["entry"]);
            if (duetState !== null) {
              yield* updateDuet({
                enabled: duetState.enabled,
                strategy: duetState.strategy,
                role: duet.role,
                participants: duetState.enabled ? (duetState.participants ?? []) : [],
              });
            }
            return;
          }
          case "context_window_changed": {
            const capacity = nonNegativeInteger(event, "contextWindow");
            if (capacity !== undefined && capacity > 0) contextWindow = capacity;
            return;
          }
          case "extension_error": {
            yield* emitExtensionError(event);
            return;
          }
          case "agent_settled": {
            if (turn?.interrupted === true) {
              if (state !== null) yield* finalizeTurn(state);
              return;
            }
            if (turn !== null) {
              turn.nativeAborted = event["aborted"] === true;
              turn.settleWhenIdle = true;
              turn.settleProbeGeneration += 1;
              if (
                turn.adoptedWake &&
                pendingPromptResponses.some(
                  (pending) =>
                    pending.providerTurnId === turn.providerTurn.id &&
                    pending.kind === "turn_start",
                )
              )
                return;
              yield* scheduleSettleProbe(turn, true);
            }
            return;
          }
          case "response": {
            const command = recordString(event, "command");
            if (command === "compact") {
              const pendingCompact = pendingCompactResponses.shift();
              const compactTurn =
                pendingCompact?.providerTurnId === turn?.providerTurn.id ? turn : null;
              if (compactTurn !== null) compactTurn.manualCompactInFlight = false;
              if (event["success"] === true) {
                if (
                  pendingCompact?.kind === "turn_start" &&
                  compactTurn !== null &&
                  compactTurn.promptMayBeCommandOnly &&
                  !compactTurn.sawAgentActivity
                ) {
                  yield* scheduleSettleProbe(compactTurn);
                }
                return;
              }
              if (event["success"] !== false) return;
              if (compactTurn === null) return;
              if (compactTurn.activeCompaction !== null) return;
              if (pendingCompact?.kind === "steer") {
                yield* Effect.logWarning("Draht rejected a compact steer.", {
                  errorLength: recordString(event, "error")?.length,
                });
                return;
              }
              if (!compactTurn.sawCompaction) {
                compactTurn.failure = makeProviderFailure({
                  message: recordString(event, "error") ?? "Draht compact failed.",
                  class: "provider_error",
                });
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              if (!compactTurn.sawAgentActivity) {
                yield* scheduleSettleProbe(compactTurn);
              }
              return;
            }
            const pendingPrompt = command === "prompt" ? pendingPromptResponses.shift() : undefined;
            const responseTurn =
              pendingPrompt?.providerTurnId === turn?.providerTurn.id ? turn : null;
            if (event["success"] === true) {
              // Deferred success ack. Command-only prompts (pure extension
              // slash commands) never start an agent run and never emit
              // `agent_settled`, so probe for idleness. The probe result is
              // re-queued behind any events Draht emitted before answering
              // get_state, which keeps the check stream-ordered.
              if (
                pendingPrompt?.kind === "turn_start" &&
                responseTurn !== null &&
                ((responseTurn.promptMayBeCommandOnly && !responseTurn.sawAgentActivity) ||
                  (responseTurn.adoptedWake &&
                    (responseTurn.settleWhenIdle || responseTurn.promptMayBeCommandOnly)))
              ) {
                yield* scheduleSettleProbe(responseTurn, responseTurn.adoptedWake);
              }
              return;
            }
            if (event["success"] !== false) return;
            if (command === "steer" || pendingPrompt?.kind === "steer") {
              // A rejected steer only means that one message was refused. The
              // turn it was aimed at is still running on Draht, so terminalizing
              // here would report a failure while output keeps streaming.
              yield* Effect.logWarning("Draht rejected a steer message.", {
                errorLength: recordString(event, "error")?.length,
              });
              return;
            }
            const failedTurn =
              command === "prompt" && pendingPrompt?.kind === "turn_start"
                ? responseTurn
                : command === "parse"
                  ? turn
                  : null;
            if (failedTurn !== null) {
              const failure = makeProviderFailure({
                message: recordString(event, "error") ?? "Draht rejected the prompt.",
                class: "provider_error",
              });
              if (failedTurn.adoptedWake && command === "prompt") {
                failedTurn.rejectedPromptFailure = failure;
                failedTurn.settleWhenIdle = true;
                yield* scheduleSettleProbe(failedTurn, true);
              } else {
                failedTurn.failure = failure;
                if (state !== null) yield* finalizeTurn(state);
              }
            }
            return;
          }
          case "t3.ui_request_expired": {
            const requestId = recordString(event, "requestId");
            if (requestId === undefined) return;
            const pending = pendingPrompts.get(requestId);
            if (pending === undefined || pending !== event["pending"]) return;
            pendingPrompts.delete(requestId);
            yield* cancelPrompt(pending, yield* DateTime.now);
            return;
          }
          case "t3.flush_extension_errors": {
            for (const extensionError of outOfTurnExtensionErrors.splice(0)) {
              yield* emitExtensionError(extensionError);
            }
            return;
          }
          case "t3.rollback_barrier": {
            if (rollbackBarrier !== null && event["barrier"] === rollbackBarrier)
              yield* Deferred.succeed(rollbackBarrier, undefined);
            return;
          }
          case "t3.settle_probe": {
            const data = event["data"];
            const probeFailed = event["probeFailed"] === true;
            const settleAfterAgentActivity = event["settleAfterAgentActivity"] === true;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            if (
              turn === null ||
              turn.providerTurn.id !== event["providerTurnId"] ||
              turn.settleProbeGeneration !== event["settleProbeGeneration"] ||
              (!settleAfterAgentActivity && turn.sawAgentActivity) ||
              turn.activeCompaction !== null
            ) {
              return;
            }
            if (probeFailed) {
              if (!settleAfterAgentActivity) {
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              if (attempt < SETTLE_PROBE_MAX_ATTEMPTS) {
                yield* Effect.sleep(SETTLE_PROBE_RETRY_DELAY).pipe(
                  Effect.andThen(scheduleSettleProbe(turn, true, attempt + 1)),
                  Effect.forkIn(scope),
                );
                return;
              }
              stopRequested = true;
              yield* connection.terminate;
              return;
            }
            if (
              recordField(data, "isStreaming") !== true &&
              recordField(data, "isCompacting") !== true &&
              (recordNumber(data, "pendingMessageCount") ?? 0) === 0
            ) {
              turn.settleWhenIdle = false;
              if (state !== null) yield* finalizeTurn(state);
            }
            return;
          }
          default:
            return;
        }
      });

      yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          yield* sessionEventPermit.withPermits(1)(handleSessionEvent(event));
        }
      }).pipe(
        Effect.catchCause((cause) =>
          sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              const state = threadState;
              if (rollbackBarrier !== null)
                yield* Deferred.fail(
                  rollbackBarrier,
                  protocolError("Draht transport closed during rollback"),
                );
              pendingWake = null;
              const interrupted = state?.activeTurn?.interrupted === true;
              if (state?.activeTurn != null) {
                state.activeTurn.failure = interrupted
                  ? null
                  : makeProviderFailure({
                      cause,
                      message: "Draht process exited unexpectedly.",
                      class: "transport_error",
                    });
                yield* finalizeTurn(state, false);
              } else {
                yield* cancelPendingPrompts(yield* DateTime.now);
              }
              // Background workflows run inside the Draht process and end with it. Clear the
              // roster before settling, as ingestion stops once no open work remains.
              if (backgroundWorkflows.length > 0) {
                backgroundWorkflows.length = 0;
                if (state !== null) yield* publishBackgroundWorkflows(state);
              }
              yield* settleSubagents(() => true, "interrupted");
              if (unsolicitedActivityDetected) {
                yield* updateProviderSession("error", DRAHT_UNSOLICITED_ACTIVITY_ERROR);
                yield* Queue.end(events);
              } else if (stopRequested) {
                yield* updateProviderSession("stopped", null);
                yield* Queue.end(events);
              } else {
                yield* updateProviderSession(
                  "error",
                  interrupted ? "Draht process was stopped." : "Draht process exited unexpectedly.",
                );
                yield* Queue.fail(
                  events,
                  new ProviderAdapter.ProviderAdapterEventStreamError({
                    driver: DRAHT_PROVIDER,
                    providerSessionId: input.providerSessionId,
                    cause,
                  }),
                );
              }
            }),
          ),
        ),
        Effect.forkIn(scope),
      );

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        publish = true,
      ) {
        if (rollbackBarrier !== null)
          return yield* protocolError("Cannot register a Draht thread during rollback");
        if (threadState !== null && threadState.activeTurn !== null) {
          return yield* protocolError("Cannot register a Draht thread while a turn is active");
        }
        if (pendingWake !== null) {
          const state = threadState;
          if (
            state !== null &&
            state === pendingWake.state &&
            state.providerThread.appThreadId === threadInput.threadId &&
            (threadInput.existingProviderThread === undefined ||
              threadInput.existingProviderThread.nativeThreadRef?.nativeId ===
                state.providerThread.nativeThreadRef?.nativeId)
          )
            return state.providerThread;
          return yield* protocolError(
            "Cannot switch Draht threads while native work is awaiting a turn",
          );
        }
        const existing = threadInput.existingProviderThread;
        const resumeId = existing?.nativeThreadRef?.nativeId;
        const needsNewSession = resumeId == null && registrationAttempted;
        registrationAttempted = true;
        if (resumeId != null || needsNewSession) {
          lastNativeThreadId = resumeId ?? lastNativeThreadId;
          // Even a failed lifecycle operation can change Draht's native session.
          // Never leave the old app binding or model defaults usable afterward.
          threadState = null;
          appliedModel = null;
          appliedThinking = null;
          appliedSessionName = null;
          baselineModel = null;
          baselineThinking = null;
          contextWindow = null;
          const result = yield* lifecycleRequest(
            resumeId != null
              ? { type: "switch_session", sessionPath: resumeId }
              : { type: "new_session" },
          );
          if (recordField(result, "cancelled") === true) {
            return yield* protocolError("A Draht extension cancelled the session switch");
          }
        }
        const stateData = yield* request({ type: "get_state" });
        if (!modelsDiscovered) {
          const modelsData = yield* request({ type: "get_available_models" }).pipe(
            Effect.orElseSucceed(() => undefined),
          );
          const models = recordField(modelsData, "models");
          if (Array.isArray(models)) {
            for (const model of models) rememberModelContextWindow(model);
            modelsDiscovered = true;
          }
        }
        contextWindow = rememberModelContextWindow(recordField(stateData, "model"));
        // Each baseline is captured independently, and only while nothing has
        // been applied yet, so a `get_state` that arrives after our own
        // selection cannot record that selection as Draht's default.
        if (baselineModel === null && appliedModel === null) {
          const stateModel = recordField(stateData, "model");
          const provider = recordString(stateModel, "provider");
          const modelId = recordString(stateModel, "id");
          if (provider !== undefined && modelId !== undefined) {
            baselineModel = { provider, modelId };
          }
        }
        if (baselineThinking === null && appliedThinking === null) {
          baselineThinking = recordString(stateData, "thinkingLevel") ?? null;
        }
        // switch_session accepts a file path, not Draht's display session UUID.
        const nativeId = recordString(stateData, "sessionFile");
        if (nativeId === undefined) {
          return yield* protocolError("get_state returned no persisted sessionFile", stateData);
        }
        if (needsNewSession && nativeId === lastNativeThreadId) {
          return yield* protocolError("Draht did not create a distinct session file");
        }
        lastNativeThreadId = nativeId;
        const createdAt = yield* DateTime.now;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                ...(needsNewSession ? { nativeConversationHeadRef: null, contextUsage: null } : {}),
                status: "idle",
                updatedAt: createdAt,
              }
            : {
                id: idAllocator.derive.providerThread({
                  driver: DRAHT_PROVIDER,
                  nativeThreadId: nativeId,
                }),
                driver: DRAHT_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: providerRef(nativeId),
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt,
                updatedAt: createdAt,
              };
        threadState = { providerThread, activeTurn: null };
        const baselineEntries = yield* request({ type: "get_entries" }).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        lastKnownLeaf = recordString(baselineEntries, "leafId") ?? null;
        leafCursorStale = baselineEntries === undefined;
        const branchDuet = latestDrahtDuetState(baselineEntries);
        duet =
          branchDuet === null || !branchDuet.enabled
            ? DRAHT_DUET_OFF
            : {
                enabled: true,
                strategy: branchDuet.strategy,
                role: undefined,
                participants: branchDuet.participants ?? [],
              };
        if (publish)
          yield* emit({
            type: "provider_thread.updated",
            driver: DRAHT_PROVIDER,
            providerThread,
          });
        return providerThread;
      });

      const applySelection = Effect.fnUntraced(function* (modelSelection: ModelSelection) {
        if (duet.enabled) return;
        const thinking = getModelSelectionStringOptionValue(modelSelection, "thinking");
        if (modelSelection.model === DRAHT_INHERIT_MODEL_SLUG) {
          if (appliedModel !== null && baselineModel !== null) {
            const restoredModel = yield* request({ type: "set_model", ...baselineModel });
            contextWindow = rememberModelContextWindow(restoredModel);
            appliedModel = null;
            const updatedAt = yield* DateTime.now;
            sessionEntity = { ...sessionEntity, model: DRAHT_INHERIT_MODEL_SLUG, updatedAt };
            yield* emit({
              type: "provider_session.updated",
              driver: DRAHT_PROVIDER,
              providerSession: sessionEntity,
            });
          }
          if (
            thinking === undefined &&
            appliedThinking !== null &&
            baselineThinking !== null &&
            appliedThinking !== baselineThinking
          ) {
            yield* request({ type: "set_thinking_level", level: baselineThinking });
            appliedThinking = null;
          }
        } else if (modelSelection.model !== appliedModel) {
          const parsed = parseDrahtModelSlug(modelSelection.model);
          if (parsed === null) {
            return yield* protocolError(
              `Draht model '${modelSelection.model}' must use provider/model format`,
            );
          }
          const selectedModel = yield* request({
            type: "set_model",
            provider: parsed.provider,
            modelId: parsed.modelId,
          });
          contextWindow = rememberModelContextWindow(selectedModel);
          appliedModel = modelSelection.model;
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, model: modelSelection.model, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: DRAHT_PROVIDER,
            providerSession: sessionEntity,
          });
        }
        if (
          thinking !== undefined &&
          thinking !== appliedThinking &&
          DRAHT_THINKING_LEVELS.has(thinking)
        ) {
          yield* request({ type: "set_thinking_level", level: thinking });
          appliedThinking = thinking;
        }
      });

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<{ type: "image"; data: string; mimeType: string }> = [];
        const extraLines: Array<string> = [];
        for (const attachment of attachments) {
          const path = host.resolveAttachmentPath(attachment);
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* fileSystem.readFile(path);
            images.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          } else {
            extraLines.push(`[Attachment saved at ${path}]`);
          }
        }
        const message = extraLines.length === 0 ? text : `${text}\n\n${extraLines.join("\n")}`;
        return { message, images };
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DRAHT_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        hasPendingBackgroundWork: Effect.sync(
          () => pendingWake !== null || backgroundWorkflows.length > 0,
        ),
        hasPendingBackgroundWorkForThread: (providerThread) =>
          Effect.sync(
            () =>
              (pendingWake !== null && pendingWake.state.providerThread.id === providerThread.id) ||
              (backgroundWorkflows.length > 0 &&
                threadState?.providerThread.id === providerThread.id),
          ),
        backgroundTasksEndWithProcess: true,
        getModelContextWindow: (selection) => {
          if (selection.instanceId !== options.instanceId) return undefined;
          const slug =
            selection.model === DRAHT_INHERIT_MODEL_SLUG
              ? baselineModel === null
                ? undefined
                : `${baselineModel.provider}/${baselineModel.modelId}`
              : selection.model;
          return slug === undefined ? undefined : modelContextWindows.get(slug);
        },
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: DRAHT_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: DRAHT_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        compactThread: (turnInput) =>
          runtime.startTurn({
            ...turnInput,
            message: { ...turnInput.message, text: "/compact" },
          }),
        startTurn: Effect.fn("DrahtAdapter.startTurn")(
          function* (turnInput) {
            if (rollbackBarrier !== null)
              return yield* protocolError("Cannot start a Draht turn during rollback");
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Draht session has no registered thread");
            }
            if (state.activeTurn !== null) {
              return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                driver: DRAHT_PROVIDER,
                detail: `Draht provider thread ${turnInput.providerThread.id} already has an active turn`,
                reason: "previous_turn_active",
              });
            }
            if (
              state.providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
            ) {
              return yield* protocolError("Draht turn requested for a different native session");
            }
            // The orchestrator adopts a fork under its already-allocated row.
            // Future session updates must retain that authoritative identity.
            const continuation =
              turnInput.message.createdBy === "agent" &&
              turnInput.message.creationSource === "provider";
            if (!continuation) yield* applySelection(turnInput.modelSelection);
            if (turnInput.appThread.title !== appliedSessionName) {
              yield* request({
                type: "set_session_name",
                name: turnInput.appThread.title,
              }).pipe(
                Effect.tap(() =>
                  Effect.sync(() => (appliedSessionName = turnInput.appThread.title)),
                ),
                Effect.ignore,
              );
            }
            // Resolved before the turn is installed: a failure here (an
            // unreadable attachment) must not leave `activeTurn` set, which
            // would reject every later turn as already active.
            // Orchestration instructions reach draht through the T3 MCP
            // extension's before_agent_start system-prompt hook, never by
            // wrapping the user text: a wrapped first message would no
            // longer start with "/" and slash commands would stop expanding.
            const compactCommand = continuation
              ? null
              : parseDrahtCompactCommand(turnInput.message.text);
            const payload =
              !continuation && compactCommand === null
                ? yield* resolvePromptPayload(turnInput.message.text, turnInput.message.attachments)
                : null;
            const startedAt = yield* DateTime.now;
            const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver: DRAHT_PROVIDER,
                nativeTurnId: syntheticNativeTurnId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const activeTurn: ActiveDrahtTurn = {
              turnInput,
              providerTurn,
              startedAt,
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              messageOrdinal: 0,
              streamItems: new Map(),
              toolArgs: new Map(),
              toolStartedAt: new Map(),
              interrupted: false,
              nativeAborted: false,
              sawAgentActivity: false,
              adoptedWake: false,
              promptMayBeCommandOnly:
                compactCommand !== null || (payload?.message.trimStart().startsWith("/") ?? false),
              latestCompactionAfterTokens: null,
              lastLiveUsedTokens: null,
              settleProbeGeneration: 0,
              settleWhenIdle: false,
              sawCompaction: false,
              manualCompactInFlight: compactCommand !== null,
              activeCompaction: null,
              activeProviderRetry: null,
              failure: null,
              rejectedPromptFailure: null,
            };
            // Only the install/send/start-event boundary excludes the event
            // pump. Earlier correlated requests must leave the pump free so
            // project trust, login, and session-switch dialogs can be shown
            // and answered instead of deadlocking the caller.
            yield* Effect.gen(function* () {
              if (
                threadState !== state ||
                state.activeTurn !== null ||
                closed ||
                stopRequested ||
                rollbackBarrier !== null ||
                state.providerThread.nativeThreadRef?.nativeId !==
                  turnInput.providerThread.nativeThreadRef?.nativeId
              ) {
                return yield* protocolError("Draht turn ownership changed before it could start");
              }
              const wake = pendingWake?.state === state ? pendingWake : null;
              if (wake !== null && wake.state.providerThread.appThreadId !== turnInput.threadId) {
                return yield* protocolError("Draht native wake belongs to a different app thread");
              }
              pendingWake = null;
              state.providerThread = turnInput.providerThread;
              state.activeTurn = activeTurn;
              activeTurn.adoptedWake = wake !== null && !continuation;
              if (compactCommand !== null) {
                yield* connection.send(compactRpcRecord(compactCommand));
                pendingCompactResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              } else if (payload !== null) {
                yield* connection.send({
                  type: "prompt",
                  message: payload.message,
                  streamingBehavior: "steer",
                  ...(payload.images.length === 0 ? {} : { images: payload.images }),
                });
                pendingPromptResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              }
              yield* emit({
                type: "provider_turn.updated",
                driver: DRAHT_PROVIDER,
                threadId: turnInput.threadId,
                providerTurn,
              });
              yield* updateProviderThread(state, {
                status: "active",
                firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateProviderSession("running", null);
              if (wake !== null) {
                for (const event of wake.events) {
                  if (!continuation && event["type"] === "agent_settled") {
                    activeTurn.nativeAborted = event["aborted"] === true;
                    activeTurn.settleWhenIdle = true;
                    continue;
                  }
                  yield* handleSessionEvent(event);
                }
              } else if (continuation) {
                yield* finalizeTurn(state);
              }
              if (outOfTurnExtensionErrors.length > 0) {
                yield* Queue.offer(connection.events, { type: "t3.flush_extension_errors" });
              }
            }).pipe(
              sessionEventPermit.withPermits(1),
              Effect.tapError(() =>
                Effect.sync(() => {
                  if (state.activeTurn === activeTurn) state.activeTurn = null;
                }),
              ),
            );
            // Draht acks `prompt` only after slash-command expansion completes,
            // and extension commands may block on user dialogs indefinitely.
            // Rejections therefore return later as id-less response records
            // handled by the event pump.
          },
          (effect, turnInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: DRAHT_PROVIDER,
                    threadId: turnInput.threadId,
                    providerThreadId: turnInput.providerThread.id,
                    runId: turnInput.runId,
                    cause,
                  }),
              ),
            ),
        ),
        steerTurn: Effect.fn("DrahtAdapter.steerTurn")(
          function* (steerInput: ProviderAdapter.ProviderAdapterV2SteerInput) {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null || turn.providerTurn.id !== steerInput.providerTurnId) {
              return yield* protocolError(`Draht turn ${steerInput.providerTurnId} is not active`);
            }
            const compactCommand = parseDrahtCompactCommand(steerInput.message.text);
            const payload =
              compactCommand === null
                ? yield* resolvePromptPayload(
                    steerInput.message.text,
                    steerInput.message.attachments,
                  )
                : null;
            // Prompt with streamingBehavior steer is atomic on Draht's side: it
            // queues during an active run and starts a new run if settlement
            // won the race. A direct `steer` sent after Draht became idle would
            // remain queued forever. Send fire-and-forget under the session
            // permit so a slash-command dialog cannot block the turn, and so
            // settlement cannot overtake the active-turn check.
            // /compact is not a prompt: Draht's compact RPC aborts the agent first.
            yield* sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (threadState?.activeTurn !== turn) {
                  return yield* protocolError(
                    `Draht turn ${steerInput.providerTurnId} is not active`,
                  );
                }
                if (compactCommand !== null) {
                  turn.manualCompactInFlight = true;
                  yield* connection.send(compactRpcRecord(compactCommand));
                  pendingCompactResponses.push({
                    providerTurnId: turn.providerTurn.id,
                    kind: "steer",
                  });
                } else if (payload !== null) {
                  yield* connection.send({
                    type: "prompt",
                    message: payload.message,
                    streamingBehavior: "steer",
                    ...(payload.images.length === 0 ? {} : { images: payload.images }),
                  });
                  pendingPromptResponses.push({
                    providerTurnId: turn.providerTurn.id,
                    kind: "steer",
                  });
                }
                turn.settleProbeGeneration += 1;
              }),
            );
          },
          (effect, steerInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: DRAHT_PROVIDER,
                    providerThreadId: steerInput.providerThread.id,
                    providerTurnId: steerInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        interruptTurn: Effect.fn("DrahtAdapter.interruptTurn")(
          function* (interruptInput) {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null && interruptInput.requestRuntimeRestart === true) {
              if (pendingWake !== null)
                yield* sessionEventPermit.withPermits(1)(stopPendingWake(pendingWake));
              // Draht has no command that cancels a detached workflow; it ends with the process.
              if (backgroundWorkflows.length > 0) {
                stopRequested = true;
                yield* connection.terminate;
              }
              return;
            }
            if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
              return yield* protocolError(
                `Draht turn ${interruptInput.providerTurnId} is not active`,
              );
            }
            turn.interrupted = true;
            if (
              interruptInput.requestRuntimeRestart === true ||
              turn.settleWhenIdle ||
              turn.activeCompaction !== null ||
              turn.manualCompactInFlight
            ) {
              // Draht's generic abort does not cancel manual compaction. Terminate
              // so Stop covers user /compact as well as detached recovery compact.
              stopRequested = true;
              if (interruptInput.requestRuntimeRestart === true && !turn.settleWhenIdle) {
                yield* request({ type: "abort" }, 2_000).pipe(Effect.ignore);
              }
              // Terminating fails every later request, so read the stopped
              // turn's session-tree refs first: rolling back past this turn
              // forks at its user entry. Holding the event permit also lets a
              // finalize that is already reading them finish before the kill.
              yield* sessionEventPermit.withPermits(1)(
                Effect.gen(function* () {
                  if (threadState?.activeTurn === turn && turn.stopTreeRefs === undefined) {
                    turn.stopTreeRefs = yield* captureTurnTreeRefs(2_000);
                  }
                  yield* connection.terminate;
                }),
              );
              return;
            }
            yield* request({ type: "abort" }).pipe(
              Effect.tapError(() => Effect.sync(() => (turn.interrupted = false))),
            );
          },
          (effect, interruptInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: DRAHT_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        respondToRuntimeRequest: Effect.fn("DrahtAdapter.respondToRuntimeRequest")(
          function* (requestInput) {
            const pending = pendingPrompts.get(String(requestInput.requestId));
            if (pending === undefined) {
              return yield* protocolError(
                `No pending Draht extension request ${requestInput.requestId}`,
              );
            }
            const answeredAt = yield* DateTime.now;
            if (
              pending.expiresAt !== null &&
              DateTime.toEpochMillis(answeredAt) >= pending.expiresAt
            ) {
              pendingPrompts.delete(String(requestInput.requestId));
              yield* cancelPrompt(pending, answeredAt);
              return yield* protocolError(
                `Draht extension request ${requestInput.requestId} has expired`,
              );
            }
            const response = drahtUiResponse(pending, requestInput.decision, requestInput.answers);
            yield* connection.send({
              type: "extension_ui_response",
              id: pending.nativeRequestId,
              ...response,
            });
            pendingPrompts.delete(String(requestInput.requestId));
            if (pending.expirationFiber !== null) yield* Fiber.interrupt(pending.expirationFiber);
            if (pending.method === "confirm" && requestInput.decision === "acceptForSession") {
              sessionApprovals.add(pending.approvalKey);
            }
            const resolvedAt = yield* DateTime.now;
            pending.runtimeRequest = {
              ...pending.runtimeRequest,
              status: "resolved",
              resolvedAt,
              ...(pending.method === "confirm" && requestInput.decision !== undefined
                ? { decision: requestInput.decision }
                : {}),
              ...(pending.method !== "confirm" && requestInput.answers !== undefined
                ? { answers: requestInput.answers }
                : {}),
            };
            yield* emit({
              type: "runtime_request.updated",
              driver: DRAHT_PROVIDER,
              threadId: pending.node.threadId,
              runtimeRequest: pending.runtimeRequest,
            });
            yield* emit({
              type: "node.updated",
              driver: DRAHT_PROVIDER,
              node: { ...pending.node, status: "completed", completedAt: resolvedAt },
            });
            yield* emit({
              type: "turn_item.updated",
              driver: DRAHT_PROVIDER,
              turnItem: {
                ...pending.turnItem,
                status: "completed",
                completedAt: resolvedAt,
                updatedAt: resolvedAt,
              },
            });
          },
          (effect, requestInput) =>
            effect.pipe(
              sessionEventPermit.withPermits(1),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: DRAHT_PROVIDER,
                    requestId: requestInput.requestId,
                    cause,
                  }),
              ),
            ),
        ),
        readThreadSnapshot: Effect.fn("DrahtAdapter.readThreadSnapshot")(
          function* (snapshotInput) {
            const state = threadState;
            const boundNativeId = state?.providerThread.nativeThreadRef?.nativeId;
            const wantedNativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
            if (state === null || wantedNativeId == null || boundNativeId !== wantedNativeId) {
              return yield* protocolError(
                "Draht snapshot requested for a thread this session does not host",
              );
            }
            // get_messages is Draht's active-branch view. get_entries returns the
            // whole session tree, including abandoned branches after /tree or
            // fork, which would leak discarded conversation into handoffs.
            const messagesData = yield* request({ type: "get_messages" });
            const activeMessages = recordField(messagesData, "messages");
            const threadId = state.providerThread.appThreadId ?? input.threadId;
            const messages = (Array.isArray(activeMessages) ? activeMessages : []).flatMap(
              (message, index) => {
                const role = recordString(message, "role");
                if (role !== "user" && role !== "assistant") return [];
                const text = contentText(recordField(message, "content"));
                if (text.length === 0) return [];
                const timestamp = recordNumber(message, "timestamp");
                const at = Option.getOrElse(
                  DateTime.make(timestamp ?? Number.NaN),
                  () => state.providerThread.createdAt,
                );
                return [
                  {
                    id: idAllocator.derive.messageFromProviderItem({
                      driver: DRAHT_PROVIDER,
                      nativeItemId: `${wantedNativeId}:snapshot-message:${index}`,
                    }),
                    threadId,
                    runId: null,
                    nodeId: null,
                    role: role as "user" | "assistant",
                    text,
                    attachments: [],
                    streaming: false,
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: "provider" as const,
                    createdAt: at,
                    updatedAt: at,
                  },
                ];
              },
            );
            return {
              providerThread: state.providerThread,
              providerTurns: [],
              messages,
              runtimeRequests: [],
            };
          },
          (effect, snapshotInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                    driver: DRAHT_PROVIDER,
                    providerThreadId: snapshotInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
        ),
        rollbackThread: Effect.fn("DrahtAdapter.rollbackThread")(
          function* (rollbackInput) {
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Draht session has no registered thread");
            }
            if (state.providerThread.id !== rollbackInput.providerThread.id) {
              return yield* protocolError(
                "Draht rollback requested for a thread this session does not host",
              );
            }
            if (state.activeTurn !== null) {
              return yield* protocolError("Cannot roll back while a Draht turn is active");
            }
            if (pendingWake !== null) {
              return yield* protocolError("Cannot roll back while native Draht work awaits a turn");
            }
            const boundary = drahtForkBoundary(rollbackInput);
            if (boundary.type === "nothing") {
              return drahtThreadSnapshot(state.providerThread);
            }
            const forkEntryId = yield* drahtForkEntry(
              boundary,
              rollbackInput.providerThread.createdAt,
              request({ type: "get_entries" }),
            );
            return yield* Effect.acquireUseRelease(
              sessionEventPermit.withPermits(1)(
                Effect.gen(function* () {
                  if (
                    rollbackBarrier !== null ||
                    threadState !== state ||
                    state.activeTurn !== null ||
                    pendingWake !== null ||
                    closed ||
                    stopRequested
                  )
                    return yield* protocolError(
                      "Draht rollback ownership changed before its fork could start",
                    );
                  const barrier = yield* Deferred.make<
                    void,
                    ProviderAdapter.ProviderAdapterProtocolError
                  >();
                  rollbackBarrier = barrier;
                  return barrier;
                }),
              ),
              (barrier) =>
                Effect.gen(function* () {
                  const forkData = yield* lifecycleRequest({ type: "fork", entryId: forkEntryId });
                  if (recordField(forkData, "cancelled") === true)
                    return yield* protocolError("A Draht extension cancelled the session fork");
                  const forkState = yield* request({ type: "get_state" }).pipe(
                    Effect.onError(() =>
                      Effect.sync(() => {
                        threadState = null;
                      }),
                    ),
                  );
                  const forkSessionFile = recordString(forkState, "sessionFile");
                  if (forkSessionFile === undefined) {
                    threadState = null;
                    return yield* protocolError(
                      "Draht fork did not return a persisted session file",
                    );
                  }
                  const entriesData = yield* request({ type: "get_entries" }).pipe(
                    Effect.orElseSucceed(() => undefined),
                  );
                  const leafId = recordString(entriesData, "leafId") ?? null;
                  // RPC responses bypass the pump. Drain preceding native events
                  // before deciding whether this fork can replace their binding.
                  const queued = yield* Queue.offer(connection.events, {
                    type: "t3.rollback_barrier",
                    barrier,
                  });
                  if (!queued)
                    return yield* protocolError("Draht transport closed during rollback");
                  yield* Deferred.await(barrier);
                  return yield* sessionEventPermit.withPermits(1)(
                    Effect.gen(function* () {
                      if (pendingWake !== null) {
                        yield* stopPendingWake(pendingWake);
                        return yield* protocolError(
                          "Native Draht work started while rollback was changing the session",
                        );
                      }
                      if (
                        threadState !== state ||
                        state.activeTurn !== null ||
                        closed ||
                        stopRequested
                      )
                        return yield* protocolError(
                          "Draht rollback ownership changed during its fork",
                        );
                      lastNativeThreadId = forkSessionFile;
                      appliedModel = null;
                      appliedThinking = null;
                      appliedSessionName = null;
                      lastKnownLeaf = leafId;
                      leafCursorStale = entriesData === undefined;
                      yield* updateProviderThread(state, {
                        nativeThreadRef: providerRef(forkSessionFile),
                        nativeConversationHeadRef: leafId === null ? null : providerRef(leafId),
                      });
                      rollbackBarrier = null;
                      return drahtThreadSnapshot(state.providerThread);
                    }),
                  );
                }).pipe(Effect.onInterrupt(() => connection.terminate)),
              (barrier) =>
                Effect.sync(() => {
                  if (rollbackBarrier === barrier) rollbackBarrier = null;
                }),
            );
          },
          (effect, rollbackInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRollbackThreadError({
                    driver: DRAHT_PROVIDER,
                    providerThreadId: rollbackInput.providerThread.id,
                    checkpointId: rollbackInput.target.checkpointId,
                    cause,
                  }),
              ),
            ),
        ),
        forkThread: Effect.fn("DrahtAdapter.forkThread")(
          function* (forkInput) {
            if (threadState?.activeTurn != null) {
              return yield* protocolError("Cannot fork while a Draht turn is active");
            }
            const source = forkInput.sourceProviderThread;
            const sourceFile = source.nativeThreadRef?.nativeId;
            if (sourceFile == null)
              return yield* protocolError("Draht fork source has no session file");
            const sourceTurns = (forkInput.sourceProviderTurns ?? []).filter(
              (turn) => turn.providerThreadId === source.id,
            );
            const target = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
            if (forkInput.providerTurnId !== undefined && target === undefined) {
              return yield* protocolError("Draht fork target turn is missing");
            }
            const boundary: DrahtForkBoundary =
              target === undefined
                ? { type: "nothing" }
                : drahtForkBoundary({
                    target: { type: "provider_turn", providerTurn: target },
                    providerThreadTurns: sourceTurns,
                  });
            // CLI --fork uses Draht's own session format and sets the destination cwd. RPC
            // switch_session/clone alone would retain the source cwd.
            const forkLaunch = buildDrahtRpcLaunch({
              launchArgs: resolvedLaunchArgs.args,
              environment: options.environment,
              mcpSession: undefined,
              extensionPath: undefined,
              disableExtensions: true,
              disableTools: true,
            });
            const nativeId = yield* Effect.scoped(
              Effect.gen(function* () {
                const forkConnection = yield* makeDrahtRpcConnection({
                  command: options.settings.binaryPath || "draht",
                  args: [...forkLaunch.args, "--fork", sourceFile],
                  cwd,
                  env: forkLaunch.env,
                });
                yield* Stream.fromQueue(forkConnection.events).pipe(
                  Stream.runDrain,
                  Effect.ignore,
                  Effect.forkScoped,
                );
                const beforeEntry =
                  boundary.type === "nothing"
                    ? null
                    : yield* drahtForkEntry(
                        boundary,
                        source.createdAt,
                        forkConnection.request({ type: "get_entries" }),
                      );
                if (beforeEntry !== null) {
                  const result = yield* forkConnection.request({
                    type: "fork",
                    entryId: beforeEntry,
                  });
                  if (recordField(result, "cancelled") === true) {
                    return yield* protocolError("A Draht extension cancelled the session fork");
                  }
                }
                const state = yield* forkConnection.request({ type: "get_state" });
                const file = recordString(state, "sessionFile");
                if (file === undefined || file === sourceFile) {
                  return yield* protocolError("Draht fork did not create a distinct session file");
                }
                return file;
              }),
            ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
            const now = yield* DateTime.now;
            return yield* registerThread(
              {
                threadId: forkInput.targetThreadId,
                modelSelection: forkInput.modelSelection ?? input.modelSelection,
                runtimePolicy: forkInput.runtimePolicy ?? input.runtimePolicy,
                existingProviderThread: {
                  ...source,
                  id: idAllocator.derive.providerThread({
                    driver: DRAHT_PROVIDER,
                    nativeThreadId: nativeId,
                  }),
                  appThreadId: forkInput.targetThreadId,
                  providerSessionId: input.providerSessionId,
                  providerInstanceId: options.instanceId,
                  ownerNodeId: forkInput.ownerNodeId ?? null,
                  nativeThreadRef: providerRef(nativeId),
                  nativeConversationHeadRef: null,
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  pendingBackgroundTasks: [],
                  forkedFrom: {
                    providerThreadId: source.id,
                    ...(forkInput.providerTurnId === undefined
                      ? {}
                      : { providerTurnId: forkInput.providerTurnId }),
                  },
                  createdAt: now,
                  updatedAt: now,
                },
              },
              false,
            );
          },
          (effect, forkInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterForkThreadError({
                    driver: DRAHT_PROVIDER,
                    providerThreadId: forkInput.sourceProviderThread.id,
                    cause,
                  }),
              ),
            ),
        ),
      };
      return runtime;
    }),
  });
});

interface DrahtSessionEntry {
  readonly parentId: string | null;
  readonly isUserMessage: boolean;
  /** Draht's `timestamp` as epoch milliseconds; null when missing or unparsable. */
  readonly timestamp: number | null;
}

interface DrahtSessionListing {
  readonly entries: ReadonlyMap<string, DrahtSessionEntry>;
  readonly leafId: string | null;
}

function drahtSessionListing(data: unknown): DrahtSessionListing | null {
  const leafId = recordField(data, "leafId");
  const entries = drahtSessionEntries(recordField(data, "entries"));
  return entries !== null && (typeof leafId === "string" || leafId === null)
    ? { entries, leafId }
    : null;
}

function drahtSessionEntries(entries: unknown): ReadonlyMap<string, DrahtSessionEntry> | null {
  const byId = new Map<string, DrahtSessionEntry>();
  if (!Array.isArray(entries)) return null;
  for (const entry of entries) {
    const id = recordString(entry, "id");
    const parentId = recordField(entry, "parentId");
    if (id === undefined || byId.has(id) || (parentId !== null && typeof parentId !== "string"))
      return null;
    const timestamp = recordString(entry, "timestamp");
    byId.set(id, {
      parentId,
      isUserMessage:
        recordField(entry, "type") === "message" &&
        recordString(recordField(entry, "message"), "role") === "user",
      timestamp:
        timestamp === undefined
          ? null
          : Option.getOrNull(Option.map(DateTime.make(timestamp), DateTime.toEpochMillis)),
    });
  }
  return byId;
}

function walkDrahtBranch(
  entries: ReadonlyMap<string, DrahtSessionEntry>,
  leafId: string | null,
  stopAt: string | null,
): {
  readonly entryCount: number;
  readonly userEntryIds: ReadonlyArray<string>;
  readonly reachedStop: boolean;
} | null {
  const visited = new Set<string>();
  const userEntryIds: Array<string> = [];
  let current = leafId;
  while (current !== null && current !== stopAt) {
    const entry = entries.get(current);
    if (entry === undefined) break;
    if (visited.has(current)) return null;
    visited.add(current);
    if (entry.isUserMessage) userEntryIds.push(current);
    current = entry.parentId;
  }
  return { entryCount: visited.size, userEntryIds, reachedStop: current === stopAt };
}

type DrahtForkBoundary =
  | { readonly type: "entry"; readonly entryId: string }
  | { readonly type: "nothing" }
  | {
      readonly type: "uncaptured";
      readonly turn: OrchestrationV2ProviderTurn;
      readonly target: OrchestrationV2ProviderTurn | null;
    };

/**
 * Turns without a ref left nothing on the active branch, so the boundary is the first discarded
 * turn that has one. `uncaptured` is a boundary turn still carrying its synthetic ref (only
 * `captureTurnTreeRefs` records strong ones); `drahtForkEntry` locates it in the session tree.
 */
function drahtForkBoundary(input: {
  readonly target:
    | { readonly type: "thread_start" }
    | { readonly type: "provider_turn"; readonly providerTurn: OrchestrationV2ProviderTurn };
  readonly providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
}): DrahtForkBoundary {
  const target = input.target.type === "thread_start" ? null : input.target.providerTurn;
  const boundary = input.providerThreadTurns
    .filter((turn) => turn.ordinal > (target?.ordinal ?? 0) && turn.nativeTurnRef !== null)
    .sort((a, b) => a.ordinal - b.ordinal)[0];
  const ref = boundary?.nativeTurnRef;
  if (boundary === undefined || ref == null) return { type: "nothing" };
  return ref.strength === "strong" && ref.nativeId !== null
    ? { type: "entry", entryId: ref.nativeId }
    : { type: "uncaptured", turn: boundary, target };
}

/** Reads the session tree only for an uncaptured boundary. */
function drahtForkEntry<E>(
  boundary: Exclude<DrahtForkBoundary, { type: "nothing" }>,
  providerThreadCreatedAt: DateTime.Utc,
  readEntries: Effect.Effect<unknown, E>,
): Effect.Effect<string, E | ProviderAdapter.ProviderAdapterProtocolError> {
  if (boundary.type === "entry") return Effect.succeed(boundary.entryId);
  return readEntries.pipe(
    Effect.flatMap((data) => {
      const listing = drahtSessionListing(data);
      const entryId =
        listing === null
          ? undefined
          : drahtUncapturedForkEntry(listing, boundary, providerThreadCreatedAt);
      return entryId === undefined
        ? Effect.fail(
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver: DRAHT_PROVIDER,
              detail: `Draht turn ${boundary.turn.ordinal} has no captured session-tree entry and none was found in the session tree`,
            }),
          )
        : Effect.succeed(entryId);
    }),
  );
}

/**
 * Draht shares the server clock. An adopted wake can precede its T3 turn, so search from the target
 * turn's end through the boundary turn's end. A boundary turn still running has no end yet, so
 * the window stays open through the tree read. At thread start, use the earlier of thread
 * creation and turn start to include adopted wakes while preserving inherited session history.
 */
function drahtUncapturedForkEntry(
  listing: DrahtSessionListing,
  boundary: Extract<DrahtForkBoundary, { type: "uncaptured" }>,
  providerThreadCreatedAt: DateTime.Utc,
): string | undefined {
  const { startedAt, completedAt } = boundary.turn;
  if (startedAt === null) return undefined;
  const from =
    boundary.target === null
      ? Math.min(DateTime.toEpochMillis(providerThreadCreatedAt), DateTime.toEpochMillis(startedAt))
      : DateTime.toEpochMillis(boundary.target.completedAt ?? startedAt);
  const to = completedAt === null ? Infinity : DateTime.toEpochMillis(completedAt);
  const targetRef = boundary.target?.nativeTurnRef;
  const stopAt = targetRef?.strength === "strong" ? targetRef.nativeId : null;
  if (stopAt !== null && !listing.entries.has(stopAt)) return undefined;
  const walk = walkDrahtBranch(listing.entries, listing.leafId, stopAt);
  if (walk === null || !walk.reachedStop) return undefined;
  let earliest: string | undefined;
  for (const id of walk.userEntryIds) {
    const timestamp = listing.entries.get(id)?.timestamp;
    if (timestamp == null) return undefined;
    if (timestamp >= from && timestamp <= to) earliest = id;
  }
  return earliest;
}

function drahtExtensionDisplayName(extensionPath: string | undefined): string {
  if (extensionPath === undefined) return "Draht extension";
  const normalized = extensionPath.replace(/\\/g, "/").replace(/\/+$/, "");
  const name = normalized.slice(normalized.lastIndexOf("/") + 1).replace(/\.[^.]+$/, "");
  return name.length === 0 ? "Draht extension" : name;
}

function drahtThreadSnapshot(
  providerThread: OrchestrationV2ProviderThread,
): ProviderAdapter.ProviderAdapterV2ThreadSnapshot {
  return { providerThread, providerTurns: [], messages: [], runtimeRequests: [] };
}

function drahtQuestion(
  questionId: string,
  method: "select" | "input" | "editor",
  title: string,
  event: DrahtRpcRecord,
): OrchestrationV2UserInputQuestion {
  const options =
    method === "select" && Array.isArray(event["options"])
      ? event["options"]
          .filter((option): option is string => typeof option === "string")
          .map((option) => ({
            label: option || "Empty value",
            description: option || "Empty value",
            value: option,
          }))
      : method === "editor"
        ? []
        : [
            {
              label: "Submit empty value",
              description: "Send an empty string to the extension.",
              value: "",
            },
          ];
  // Extensions may send a blank placeholder, which is not a valid question.
  const question =
    [recordString(event, "message"), recordString(event, "placeholder")].find(
      (text) => text !== undefined && text.trim().length > 0,
    ) ?? title;
  return {
    id: questionId,
    header: title,
    question,
    ...(method === "editor" ? { initialAnswer: recordString(event, "prefill") ?? "" } : {}),
    options,
  };
}

function drahtUiResponse(
  pending: PendingDrahtPrompt,
  decision: ProviderApprovalDecision | undefined,
  answers: Record<string, unknown> | undefined,
): DrahtRpcRecord {
  if (pending.method === "confirm") {
    // Where Draht offered options, the chosen option's id decides, not `confirmed`.
    const optionId = (id: string | undefined) => (id === undefined ? {} : { optionId: id });
    if (decision === "accept" || decision === "acceptForSession") {
      return { confirmed: true, ...optionId(pending.optionIds.accept) };
    }
    if (decision === "decline") return { confirmed: false, ...optionId(pending.optionIds.decline) };
    return { cancelled: true };
  }
  const answer = answers?.[pending.questionId];
  // An empty string is a valid dialog value per the RPC spec (the extension
  // receives ""), distinct from cancelling (the extension receives undefined).
  if (typeof answer === "string") return { value: answer };
  return { cancelled: true };
}

export type DrahtAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderHost.ProviderHost;

export const DrahtAdapterV2Driver: ProviderAdapterDriver<DrahtSettings, DrahtAdapterV2DriverEnv> = {
  driverKind: DRAHT_DRIVER_KIND,
  configSchema: DrahtSettings,
  defaultConfig: (): DrahtSettings => DEFAULT_DRAHT_SETTINGS,
  create: Effect.fn("DrahtAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<DrahtSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      return yield* makeDrahtAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        continuationRequests,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: DRAHT_DRIVER_KIND,
              instanceId: input.instanceId,
              detail: "Failed to create Draht adapter.",
              cause,
            }),
        ),
      ),
  ),
};
