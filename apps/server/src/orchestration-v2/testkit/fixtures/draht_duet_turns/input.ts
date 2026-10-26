import type { OrchestratorFixtureInput } from "../shared.ts";

const DRAHT_DUET_TURNS_COMMAND =
  "/duet turns lead=anthropic/claude-haiku-4-5 scout=anthropic/claude-haiku-4-5-20251001";

export function drahtDuetTurnsInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: DRAHT_DUET_TURNS_COMMAND },
      { type: "message", text: "Reply with exactly the word one." },
      { type: "message", text: "Reply with exactly the word two." },
    ],
  };
}
