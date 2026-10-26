import { DRAHT_POLYPHASE_WORKSPACE_FILES } from "../draht_subagents/input.ts";
import { DRAHT_WORKFLOW_COMMAND } from "../draht_workflow_command/input.ts";
import type { OrchestratorFixtureInput } from "../shared.ts";

export function drahtWorkflowCommandStopInput(): OrchestratorFixtureInput {
  return {
    workspaceFiles: DRAHT_POLYPHASE_WORKSPACE_FILES,
    steps: [
      { type: "message", text: DRAHT_WORKFLOW_COMMAND },
      { type: "stop_background_work", targetRunIndex: 1 },
    ],
  };
}
