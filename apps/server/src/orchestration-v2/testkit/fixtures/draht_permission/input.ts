import type { OrchestratorFixtureInput } from "../shared.ts";

export const DRAHT_PERMISSION_APPROVED_FILE = "draht-approved.txt";
export const DRAHT_PERMISSION_DECLINED_FILE = "draht-declined.txt";
export const DRAHT_PERMISSION_WRITTEN_FILE = "draht-written.txt";

export function drahtPermissionInput(): OrchestratorFixtureInput {
  return {
    runtimeMode: "approval-required",
    steps: [
      {
        type: "message",
        text: `Run the shell command \`touch ${DRAHT_PERMISSION_APPROVED_FILE}\` with your bash tool, then reply DONE.`,
      },
      { type: "approve_next_runtime_request" },
      {
        type: "message",
        text: `Run the shell command \`touch ${DRAHT_PERMISSION_DECLINED_FILE}\` with your bash tool once. If it is rejected, do not retry; reply REJECTED.`,
      },
      { type: "approve_next_runtime_request", decision: "decline" },
      {
        type: "message",
        text: `Use your write tool once to create ${DRAHT_PERMISSION_WRITTEN_FILE} containing exactly: approved. Do not use any other tool, then reply WRITTEN.`,
      },
      { type: "approve_next_runtime_request" },
    ],
  };
}
