import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";

/**
 * The thread lists the detached workflow as background work until its finished run arrives as a
 * custom message, which completes the workflow subagent the command's run started.
 */
export function assertDrahtWorkflowCommandOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assert.lengthOf(projection.runs, 1);
  assert.equal(projection.runs[0]?.status, "completed");

  assert.lengthOf(projection.subagents, 1);
  const [workflow] = projection.subagents;
  assert.equal(workflow?.status, "completed");
  assert.equal(workflow?.runId, projection.runs[0]?.id);
  assert.equal(workflow?.prompt, "two-scouts");
  assert.match(workflow?.workflow?.runHandles?.runId ?? "", /^cmd-/);
  assert.deepEqual(
    workflow?.workflow?.agents.map((agent) => [agent.label, agent.state]),
    [
      ["files", "completed"],
      ["readme", "completed"],
    ],
  );
  assert.isTrue(
    projection.turnItems.some(
      (item) =>
        item.type === "system_notice" && item.message.startsWith("Started workflow two-scouts"),
    ),
  );

  const rosterSizes = result.domainEvents.flatMap((event) =>
    event.type === "provider-thread.updated"
      ? [event.payload.pendingBackgroundTasks?.length ?? 0]
      : [],
  );
  assert.include(rosterSizes, 1);
  assert.equal(rosterSizes.at(-1), 0);
}
