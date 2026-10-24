import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";
import {
  DRAHT_PERMISSION_APPROVED_FILE,
  DRAHT_PERMISSION_DECLINED_FILE,
  DRAHT_PERMISSION_WRITTEN_FILE,
} from "./input.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Supervised runs Draht with its own `ask` permission mode: Draht's gate raises
 * the shell confirmations, with structured detail, and the T3 extension adds
 * the in-project write confirmation that `ask` would allow silently.
 */
export function assertDrahtPermissionOutput(
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

  const requests = projection.runtimeRequests.toSorted((left, right) =>
    left.createdAt < right.createdAt ? -1 : 1,
  );
  assert.deepEqual(
    requests.map((request) => [request.kind, request.status, request.decision]),
    [
      ["command", "resolved", "accept"],
      ["command", "resolved", "decline"],
      ["command", "resolved", "accept"],
    ],
  );
  assert.deepEqual(
    projection.turnItems.flatMap((item) =>
      item.type === "approval_request" ? [item.requestKind] : [],
    ),
    ["command", "command", "file-change"],
  );

  const confirms = transcript.entries.flatMap((entry) =>
    entry.type === "emit_inbound" &&
    isRecord(entry.frame) &&
    entry.frame.type === "extension_ui_request" &&
    entry.frame.method === "confirm"
      ? [entry.frame]
      : [],
  );
  assert.deepEqual(
    confirms.map((frame) => {
      const detail = isRecord(frame.detail) ? frame.detail : undefined;
      return [detail?.kind, detail?.toolName];
    }),
    [
      ["tool_permission", "bash"],
      ["tool_permission", "bash"],
      [undefined, undefined],
    ],
  );
  assert.equal(confirms[2]?.title, "Allow write?");

  const answers = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" &&
    isRecord(entry.frame) &&
    entry.frame.type === "extension_ui_response"
      ? [entry.frame.confirmed]
      : [],
  );
  assert.deepEqual(answers, [true, false, true]);

  const shells = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.isTrue(
    shells.some(
      (item) =>
        item.type === "command_execution" &&
        item.input.includes(DRAHT_PERMISSION_APPROVED_FILE) &&
        item.status === "completed",
    ),
  );
  assert.isFalse(
    shells.some(
      (item) =>
        item.type === "command_execution" &&
        item.input.includes(DRAHT_PERMISSION_DECLINED_FILE) &&
        item.status === "completed",
    ),
  );
  assert.isTrue(
    projection.turnItems.some(
      (item) =>
        item.type === "file_change" &&
        item.fileName === DRAHT_PERMISSION_WRITTEN_FILE &&
        item.status === "completed",
    ),
  );
}
