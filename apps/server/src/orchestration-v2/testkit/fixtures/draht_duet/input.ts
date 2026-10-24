import type { OrchestratorFixtureInput } from "../shared.ts";

const DRAHT_DUET_COMMAND =
  "/duet triage lead=anthropic/claude-haiku-4-5 scout=anthropic/claude-haiku-4-5-20251001";
const DRAHT_DUET_PROMPT =
  "Delegate one task to the scout teammate with duet_delegate: list the files in the current directory. Then summarize its answer in one sentence.";

export function drahtDuetInput(): OrchestratorFixtureInput {
  return {
    workspaceFiles: { "README.md": "# Fixture\n\nA tiny workspace for Draht duet fixtures.\n" },
    steps: [
      { type: "message", text: DRAHT_DUET_COMMAND },
      { type: "message", text: DRAHT_DUET_PROMPT },
    ],
  };
}
