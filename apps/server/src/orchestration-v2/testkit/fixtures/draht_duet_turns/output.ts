import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";

/** Duet turns strategy rotates the answering model; each turn names its participant once. */
export function assertDrahtDuetTurnsOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertBaseProjection({
    result,
    transcript,
    runCount: 3,
    runStatuses: ["completed", "completed", "completed"],
  });
  const notices = projection.turnItems.flatMap((item) =>
    item.type === "system_notice" && item.message.startsWith("Duet turn: ") ? [item] : [],
  );
  assert.deepEqual(
    notices.map((item) => [item.runId, item.message]),
    [
      [projection.runs[1]?.id ?? null, "Duet turn: lead (anthropic/claude-haiku-4-5)"],
      [projection.runs[2]?.id ?? null, "Duet turn: scout (anthropic/claude-haiku-4-5-20251001)"],
    ],
  );
}
