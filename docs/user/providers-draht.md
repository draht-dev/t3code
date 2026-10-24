# Draht

T3 Code can use your existing [Draht](https://draht.dev) coding agent installation while keeping
Draht's models, auth, permission rules, extensions, skills, context files, and native session
history.

## Set Up Draht

1. Install Draht on the machine running the T3 Code server. Draht requires Node.js 22.23.4,
   24.22.1, 26.12.1, or a newer release of those lines. T3 Code supports Draht 2026.10.16 or newer.
2. Run `draht` in a terminal, then `/login` to connect your model provider or configure its API
   key.
3. Open T3 Code Settings, enable Draht, and refresh the provider.

For an npm installation, use:

```sh
npm install -g @draht/coding-agent
```

If `draht` is not on the server's `PATH`, set Draht's binary path to the executable. Provider
environment variables and launch arguments are also available for installations that need a custom
agent directory, context root, or model configuration. `--provider` must be paired with `--model`.
T3 Code rejects launch arguments that change Draht's execution mode, select a session, or attach to
another session because T3 owns those parts of the process lifecycle.

Update npm-managed Draht with `draht update --self`. If another package manager owns the
installation, such as a Nix profile, update it through that manager instead.

## What Carries Over

T3 Code discovers the models reported by Draht and exposes their supported thinking levels. Threads
use Draht's native session files for resume, rollback, and forks within the same Draht instance.
Switching providers uses portable conversation context. Extension dialogs appear in the T3 Code
composer, and the composer context meter follows Draht's own usage reporting.

Draht skills appear in the composer's `$` menu, including the project skills Draht loads for the
current workspace.

Draht loads its normal user and project extensions. Blocking `select`, `confirm`, `input`, and
`editor` dialogs work in T3 Code. Notifications appear in the work log. Draht terminal decoration
such as titles, status lines, and widgets does not have a T3 Code equivalent.

Subagent and workflow runs appear as tool calls in the thread, and the approvals their agents need
reach you in the conversation.

## Permission Modes

Draht's own permission rules enforce the composer permission mode. Rules in
`~/.draht/agent/permissions.yml` and a trusted project's `.draht/permissions.yml` apply in every
mode, and their deny rules always block.

- **Supervised** asks before commands, file changes, and tools that no rule covers.
- **Auto-accept edits** allows file changes inside the project, and asks before commands and tools
  that no rule covers.
- **Auto** also runs commands that pass Draht's danger filter, and starts workflow and duet runs
  without asking.
- **Full access** allows every tool that no deny rule blocks.

Tools without a matching rule include T3 Code's own tools, so the stricter modes ask before them
too. Add an `allow` rule for a tool you want to run without a prompt.

Changing the mode restarts the Draht provider session and resumes the same native conversation.

## Troubleshooting

- If Draht is unavailable, confirm that the configured binary runs on the server machine, then
  refresh the provider in Settings.
- If no models appear, open Draht directly and confirm its authentication and model configuration.
- If discovery cannot complete, T3 Code keeps Draht available with the `Draht default` model. Start
  a thread to let the interactive Draht session handle any startup prompt.
- If a project extension, skill, or permission rule is missing, trust the project in Draht, then
  start a fresh provider session.
