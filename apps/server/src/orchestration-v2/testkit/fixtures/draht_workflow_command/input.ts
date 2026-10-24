import { DRAHT_POLYPHASE_WORKSPACE_FILES } from "../draht_subagents/input.ts";
import type { OrchestratorFixtureInput } from "../shared.ts";

export const DRAHT_WORKFLOW_COMMAND = "/workflow two-scouts";

/** `/workflow` returns once the run starts; the run reports after the turn has settled. */
export function drahtWorkflowCommandInput(): OrchestratorFixtureInput {
  return {
    workspaceFiles: DRAHT_POLYPHASE_WORKSPACE_FILES,
    steps: [{ type: "message", text: DRAHT_WORKFLOW_COMMAND }, { type: "await_background_work" }],
  };
}
