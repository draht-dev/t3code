import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Stopping the thread's background work ends the Draht process, the only way to cancel a
 * detached workflow, and leaves the workflow interrupted with nothing listed as still running.
 */
export function assertDrahtWorkflowCommandStopOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assert.lengthOf(projection.subagents, 1);
  assert.equal(projection.subagents[0]?.status, "interrupted");
  const rosterSizes = result.domainEvents.flatMap((event) =>
    event.type === "provider-thread.updated"
      ? [event.payload.pendingBackgroundTasks?.length ?? 0]
      : [],
  );
  assert.include(rosterSizes, 1);
  assert.equal(rosterSizes.at(-1), 0);
  assert.isFalse(
    transcript.entries.some(
      (entry) =>
        entry.type === "emit_inbound" &&
        isRecord(entry.frame) &&
        entry.frame.type === "message_end" &&
        isRecord(entry.frame.message) &&
        entry.frame.message.customType === "polyphase-workflow",
    ),
    "the stopped workflow must not report a result",
  );
}
