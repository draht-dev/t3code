import { assert, describe, it } from "@effect/vitest";

import {
  drahtAgentResult,
  drahtChildApprovalLabel,
  drahtRunPrompt,
  drahtWorkflowProgress,
  latestDrahtDuetState,
  parseDrahtDuetActivity,
  parseDrahtDuetExitCodes,
  parseDrahtDuetSections,
  parseDrahtDuetStatus,
  parseDrahtPolyphaseDetails,
  parseDrahtStartedWorkflow,
} from "./polyphase.ts";

// Recorded from Draht 2026.10.16-1 running `/workflow two-scouts`.
const WORKFLOW_COMMAND_DETAILS = {
  v: 1,
  runId: "cmd-4919c8f8-831a-4b75-b1c6-79c72a5bb198",
  kind: "workflow",
  origin: "command",
  title: "two-scouts",
  status: "done",
  startedAt: 1792850278851,
  agents: [
    {
      i: 0,
      label: "files",
      agent: "scout",
      status: "done",
      task: "List the files in the current directory with the ls tool. End with STATUS: done.",
      model: "anthropic/claude-haiku-4-5",
      phase: 0,
      thinking: "high",
      startedAt: 1792850278885,
      endedAt: 1792850283695,
      turns: 2,
      toolCalls: 1,
      tools: { ls: 1 },
      tokens: 7654,
      statusLine: "STATUS: done",
      output: "The current directory contains README.md.\n\nSTATUS: done",
    },
    {
      i: 1,
      label: "readme",
      agent: "scout",
      status: "running",
      task: "Read README.md with the read tool and summarize it in one sentence.",
      model: "anthropic/claude-haiku-4-5",
      phase: 0,
      startedAt: 1792850278889,
      toolCalls: 1,
      tokens: 7771,
      now: "thinking · The README.md is",
    },
  ],
  totals: { agents: 2, tokens: 15425, cost: 0.017557, byStatus: { done: 1 }, blockedToolCalls: 0 },
  degraded: 0,
  endedAt: 1792850283698,
  workflow: {
    name: "two-scouts",
    description: "Two scouts look at the workspace in parallel",
    source: "project",
    path: "/repo/.draht/workflows/two-scouts.js",
    args: "",
  },
  phases: [{ title: "Scout" }, { title: "Report" }],
  currentPhase: 1,
  log: ["scouts finished"],
  resultPreview: '{"files":"The current directory contains README.md."}',
};

describe("Draht polyphase details", () => {
  it("maps a workflow run onto workflow phases and member agents", () => {
    const details = parseDrahtPolyphaseDetails(WORKFLOW_COMMAND_DETAILS);
    assert.isNotNull(details);
    if (details === null) return;
    const workflow = drahtWorkflowProgress(details, undefined);
    assert.deepEqual(workflow.phases, [
      { index: 0, title: "Scout" },
      { index: 1, title: "Report" },
    ]);
    assert.deepEqual(workflow.runHandles, {
      runId: "cmd-4919c8f8-831a-4b75-b1c6-79c72a5bb198",
      scriptPath: "/repo/.draht/workflows/two-scouts.js",
    });
    assert.deepEqual(workflow.agents[0], {
      index: 0,
      label: "files",
      state: "completed",
      phaseIndex: 0,
      phaseTitle: "Scout",
      model: "anthropic/claude-haiku-4-5",
      totalTokens: 7654,
      toolCalls: 1,
      durationMs: 4810,
      startedAt: 1792850278885,
      completedAt: 1792850283695,
      prompt: "List the files in the current directory with the ls tool. End with STATUS: done.",
      result: "The current directory contains README.md.\n\nSTATUS: done",
    });
    // A running agent reports no end, and its live activity is not a result.
    assert.equal(workflow.agents[1]?.state, "running");
    assert.isUndefined(workflow.agents[1]?.completedAt);
    assert.isUndefined(workflow.agents[1]?.result);
    assert.equal(workflow.totalTokens, 15425);
    assert.equal(workflow.durationMs, 4847);
    assert.equal(drahtRunPrompt(details), "two-scouts");
  });

  it("keeps agents a bounded update left out at their last reported state", () => {
    const first = parseDrahtPolyphaseDetails(WORKFLOW_COMMAND_DETAILS);
    const trimmed = parseDrahtPolyphaseDetails({
      ...WORKFLOW_COMMAND_DETAILS,
      agents: [WORKFLOW_COMMAND_DETAILS.agents[1]],
      agentsOmitted: { count: 1, byStatus: { done: 1 } },
    });
    if (first === null || trimmed === null) return assert.fail("details should parse");
    const workflow = drahtWorkflowProgress(trimmed, drahtWorkflowProgress(first, undefined));
    assert.deepEqual(
      workflow.agents.map((agent) => [agent.label, agent.state]),
      [
        ["files", "completed"],
        ["readme", "running"],
      ],
    );
  });

  it("reads an unknown status as still running and refuses other detail versions", () => {
    const details = parseDrahtPolyphaseDetails({
      ...WORKFLOW_COMMAND_DETAILS,
      agents: [{ ...WORKFLOW_COMMAND_DETAILS.agents[0], status: "paused" }],
    });
    assert.equal(
      details === null ? undefined : drahtWorkflowProgress(details, undefined).agents[0]?.state,
      "running",
    );
    assert.isNull(parseDrahtPolyphaseDetails({ ...WORKFLOW_COMMAND_DETAILS, v: 2 }));
    assert.isNull(parseDrahtPolyphaseDetails({ results: [] }));
  });

  it("says why a cancelled or skipped agent stopped", () => {
    const agent = {
      i: 2,
      label: "verifier",
      agent: "verifier",
      task: "check the build",
      output: "partial findings",
    };
    assert.equal(
      drahtAgentResult({ ...agent, status: "cancelled", cancelReason: "budget" }),
      "Cancelled (budget): partial findings",
    );
    assert.equal(drahtAgentResult({ ...agent, status: "skipped", output: undefined }), "Skipped");
  });
});

describe("Draht duet", () => {
  // Recorded from Draht 2026.10.23 duet triage.
  const RESULT_TEXT =
    "Duet delegation: 1/2 completed\n\n## scout (completed)\n\nThe directory contains README.md." +
    "\n\n---\n\n## critic (failed)\n\nmodel unavailable";

  it("splits a finished delegation into each teammate's section", () => {
    assert.deepEqual(
      [...parseDrahtDuetSections(RESULT_TEXT)],
      [
        ["scout", "The directory contains README.md."],
        ["critic", "model unavailable"],
      ],
    );
    assert.deepEqual(
      [
        ...parseDrahtDuetExitCodes({
          results: [
            { participant: "scout", exitCode: 0 },
            { participant: "critic", exitCode: 1 },
          ],
        }),
      ],
      [
        ["scout", 0],
        ["critic", 1],
      ],
    );
  });

  it("reads each running teammate's activity line", () => {
    assert.deepEqual(
      [...parseDrahtDuetActivity("scout: ls /repo\ncritic: thinking · the build")],
      [
        ["scout", "ls /repo"],
        ["critic", "thinking · the build"],
      ],
    );
  });

  it("reads the status line Draht shows while duet is on", () => {
    assert.deepEqual(parseDrahtDuetStatus("duet:triage lead"), {
      strategy: "triage",
      role: "lead",
    });
    assert.deepEqual(parseDrahtDuetStatus("duet:turns"), { strategy: "turns", role: undefined });
    assert.isNull(parseDrahtDuetStatus(undefined));
  });

  it("finds the duet state on the active branch, not an abandoned one", () => {
    const state = (enabled: boolean, modelRef: string) => ({
      type: "custom",
      customType: "duet-state",
      data: {
        version: 1,
        enabled,
        strategy: "triage",
        participants: [{ role: "lead", modelRef }],
      },
    });
    const listing = {
      leafId: "c",
      entries: [
        { id: "a", parentId: null, ...state(true, "anthropic/claude-haiku-4-5") },
        { id: "abandoned", parentId: "a", ...state(false, "unused/model") },
        { id: "b", parentId: "a", type: "message" },
        { id: "c", parentId: "b", type: "message" },
      ],
    };
    assert.deepEqual(latestDrahtDuetState(listing)?.participants, [
      { role: "lead", modelRef: "anthropic/claude-haiku-4-5" },
    ]);
    assert.isNull(latestDrahtDuetState({ leafId: "b", entries: listing.entries.slice(2) }));
  });
});

describe("Draht workflow commands and child approvals", () => {
  it("reads the workflow a command started, with or without the inspector hint", () => {
    assert.equal(parseDrahtStartedWorkflow("Started workflow two-scouts"), "two-scouts");
    assert.equal(
      parseDrahtStartedWorkflow("Started workflow review-pr · ctrl+o to inspect"),
      "review-pr",
    );
    assert.isNull(parseDrahtStartedWorkflow("Checkpoints are off"));
  });

  it("names the agent that forwarded a child's permission ask", () => {
    const ask = (toolCallId: string) => ({ kind: "tool_permission", toolCallId, toolName: "bash" });
    assert.equal(drahtChildApprovalLabel(ask("runner/toolu_01PERW")), "runner");
    assert.equal(drahtChildApprovalLabel(ask("files/nested/call-1")), "files");
    // The session's own tool calls carry a bare provider id.
    assert.isUndefined(drahtChildApprovalLabel(ask("toolu_01PERW")));
  });
});
