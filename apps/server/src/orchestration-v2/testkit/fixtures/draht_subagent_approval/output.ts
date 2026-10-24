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
 * A child's command reaches this session through Draht's own gate. Its approval sits under the
 * subagent that asked, offers Draht's options, and answers with Draht's option id.
 */
export function assertDrahtSubagentApprovalOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  assert.lengthOf(projection.subagents, 1);
  const [runner] = projection.subagents;
  assert.equal(runner?.title, "runner");
  assert.equal(runner?.status, "completed");
  assert.include(runner?.result ?? "", "draht-child-ok");

  assert.lengthOf(projection.runtimeRequests, 1);
  const [request] = projection.runtimeRequests;
  assert.deepEqual(
    [request?.kind, request?.status, request?.decision],
    ["command", "resolved", "accept"],
  );
  assert.equal(
    projection.nodes.find((node) => node.id === request?.nodeId)?.parentNodeId,
    runner?.id,
  );
  const approval = projection.turnItems.find((item) => item.type === "approval_request");
  assert.isTrue(approval?.type === "approval_request" && approval.requestKind === "command");
  assert.deepEqual(approval?.type === "approval_request" ? approval.options : undefined, [
    { decision: "accept", label: "Yes" },
    { decision: "decline", label: "No" },
  ]);

  const asks = transcript.entries.flatMap((entry) =>
    entry.type === "emit_inbound" &&
    isRecord(entry.frame) &&
    entry.frame.type === "extension_ui_request" &&
    entry.frame.method === "confirm"
      ? [entry.frame.title]
      : [],
  );
  assert.deepEqual(asks, ["Approve bash for subagent runner?"]);
  const answers = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" &&
    isRecord(entry.frame) &&
    entry.frame.type === "extension_ui_response"
      ? [[entry.frame.confirmed, entry.frame.optionId]]
      : [],
  );
  assert.deepEqual(answers, [[true, "approve"]]);
}
