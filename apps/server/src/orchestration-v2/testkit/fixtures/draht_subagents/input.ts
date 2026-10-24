import type { OrchestratorFixtureInput } from "../shared.ts";

const SCOUT_AGENT = `---
name: scout
description: Looks at the workspace with read-only tools and reports briefly
tools: read, ls
model: anthropic/claude-haiku-4-5
---
You are a scout. Do exactly the task, using only read-only tools, and answer in one or two sentences. End with a line \`STATUS: done\`.
`;

const RUNNER_AGENT = `---
name: runner
description: Runs one shell command and reports its output
tools: bash
model: anthropic/claude-haiku-4-5
---
Run exactly the command you are given with the bash tool, then report its output in one sentence. End with a line \`STATUS: done\`.
`;

const TWO_SCOUTS_WORKFLOW = `export const meta = {
  name: "two-scouts",
  description: "Two scouts look at the workspace in parallel, then a reporter summarizes",
  phases: [{ title: "Scout" }, { title: "Report" }],
};
phase("Scout");
const [files, readme] = await parallel([
  () => agent("List the files in the current directory with the ls tool. End with STATUS: done.", { label: "files", agentType: "scout" }),
  () => agent("Read README.md with the read tool and summarize it in one sentence. End with STATUS: done.", { label: "readme", agentType: "scout" }),
]);
phase("Report");
log("scouts finished");
return { files, readme };
`;

/** Project agents and workflows Draht loads because the recorder trusts the workspace. */
export const DRAHT_POLYPHASE_WORKSPACE_FILES = {
  "README.md": "# Fixture\n\nA tiny workspace for Draht polyphase fixtures.\n",
  ".draht/agents/scout.md": SCOUT_AGENT,
  ".draht/agents/runner.md": RUNNER_AGENT,
  ".draht/workflows/two-scouts.js": TWO_SCOUTS_WORKFLOW,
};

const DRAHT_SUBAGENTS_PROMPT =
  "Use the subagent tool once in parallel mode with the scout agent for two tasks: (1) list the files in the current directory, (2) summarize README.md. Then reply DONE.";

/** The `polyphase` keyword makes Draht offer its workflow tool for this prompt. */
const DRAHT_WORKFLOW_TOOL_PROMPT =
  "polyphase: run the saved workflow two-scouts once with the workflow tool, then reply DONE.";

export function drahtSubagentsInput(): OrchestratorFixtureInput {
  return {
    workspaceFiles: DRAHT_POLYPHASE_WORKSPACE_FILES,
    steps: [
      { type: "message", text: DRAHT_SUBAGENTS_PROMPT },
      { type: "message", text: DRAHT_WORKFLOW_TOOL_PROMPT },
    ],
  };
}
