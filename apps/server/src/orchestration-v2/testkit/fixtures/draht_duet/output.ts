import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Triage teammates appear as subagents of the lead's `duet_delegate` call, with the model their
 * duet role names and the section of the delegation result they wrote.
 */
export function assertDrahtDuetOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });

  assert.lengthOf(projection.subagents, 1);
  const [scout] = projection.subagents;
  assert.equal(scout?.title, "scout");
  assert.equal(scout?.status, "completed");
  assert.equal(scout?.model, "anthropic/claude-haiku-4-5-20251001");
  assert.equal(scout?.runId, projection.runs[1]?.id);
  assert.isAbove((scout?.result ?? "").length, 0);
  assert.equal(projection.nodes.find((node) => node.id === scout?.parentNodeId)?.kind, "tool_call");

  const duetEntries = transcript.entries.filter(
    (entry) =>
      entry.type === "emit_inbound" &&
      isRecord(entry.frame) &&
      entry.frame.type === "entry_appended" &&
      isRecord(entry.frame.entry) &&
      entry.frame.entry.customType === "duet-state",
  );
  assert.lengthOf(duetEntries, 1);
  // Draht announces the new configuration itself; triage has no per-turn participant notice.
  assert.isFalse(
    projection.turnItems.some(
      (item) => item.type === "system_notice" && item.message.startsWith("Duet turn: "),
    ),
  );
}
