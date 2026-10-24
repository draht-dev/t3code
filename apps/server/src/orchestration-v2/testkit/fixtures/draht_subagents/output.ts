import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";

/**
 * A `subagent` run reports each agent as its own subagent; a `workflow` run is one subagent whose
 * members are its agents, grouped by the workflow's phases. Both hang off the tool call that
 * started them, and Draht children have no transcripts T3 could open as threads.
 */
export function assertDrahtSubagentsOutput(
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

  const scouts = projection.subagents.filter((subagent) => subagent.workflow === undefined);
  assert.lengthOf(scouts, 2);
  for (const scout of scouts) {
    assert.match(scout.title ?? "", /^scout/);
    assert.equal(scout.status, "completed");
    assert.equal(scout.model, "anthropic/claude-haiku-4-5");
    assert.equal(scout.runId, projection.runs[0]?.id);
    assert.isNull(scout.childThreadId);
    assert.include(scout.result ?? "", "STATUS: done");
  }

  const workflows = projection.subagents.filter((subagent) => subagent.workflow !== undefined);
  assert.lengthOf(workflows, 1);
  const [workflow] = workflows;
  assert.equal(workflow?.status, "completed");
  assert.equal(workflow?.runId, projection.runs[1]?.id);
  assert.equal(workflow?.workflow?.name, "two-scouts");
  assert.deepEqual(workflow?.workflow?.phases, [
    { index: 0, title: "Scout" },
    { index: 1, title: "Report" },
  ]);
  assert.deepEqual(
    workflow?.workflow?.agents.map((agent) => [agent.label, agent.state, agent.phaseTitle]),
    [
      ["files", "completed", "Scout"],
      ["readme", "completed", "Scout"],
    ],
  );

  for (const subagent of projection.subagents) {
    assert.equal(projection.nodes.find((node) => node.id === subagent.id)?.kind, "subagent");
    assert.equal(
      projection.nodes.find((node) => node.id === subagent.parentNodeId)?.kind,
      "tool_call",
    );
  }
  assert.lengthOf(
    projection.turnItems.filter((item) => item.type === "subagent"),
    projection.subagents.length,
  );
}
