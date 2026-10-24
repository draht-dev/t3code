import { DRAHT_POLYPHASE_WORKSPACE_FILES } from "../draht_subagents/input.ts";
import type { OrchestratorFixtureInput } from "../shared.ts";

const DRAHT_SUBAGENT_APPROVAL_PROMPT =
  "Use the subagent tool once (single mode) with the runner agent and the task: run `echo draht-child-ok`. Then reply DONE.";

export function drahtSubagentApprovalInput(): OrchestratorFixtureInput {
  return {
    runtimeMode: "approval-required",
    workspaceFiles: DRAHT_POLYPHASE_WORKSPACE_FILES,
    steps: [
      { type: "message", text: DRAHT_SUBAGENT_APPROVAL_PROMPT },
      { type: "approve_next_runtime_request" },
    ],
  };
}
