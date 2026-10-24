import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

import {
  T3_MCP_BEARER_ENV,
  T3_MCP_URL_ENV,
  T3_DRAHT_RUNTIME_MODE_ENV,
} from "./mcpExtensionSource.ts";
import { buildDrahtRpcLaunch, resolveDrahtLaunchArgs } from "./mcpInjection.ts";

const threadId = ThreadId.make("thread-draht-t3-mcp");

const mcpSession = {
  environmentId: EnvironmentId.make("environment-draht-t3-mcp"),
  threadId,
  providerSessionId: "mcp-session-draht",
  providerInstanceId: ProviderInstanceId.make("draht"),
  endpoint: "http://127.0.0.1:43123/mcp",
  authorizationHeader: "Bearer secret-draht-token",
  browserToolsAvailable: true,
};

describe("draht T3 MCP injection", () => {
  it("always adds the T3 extension and configures MCP when available", () => {
    const resolvedArgs = resolveDrahtLaunchArgs(
      "--extension=/home/user/.draht/agent/extensions/demo.ts --session-dir=/tmp/draht-sessions --provider=anthropic --model=claude-sonnet --tools='' --name=-review --extension-flag=kept",
    );
    assert.isTrue(resolvedArgs.ok);
    if (!resolvedArgs.ok) return;
    const launch = buildDrahtRpcLaunch({
      launchArgs: resolvedArgs.args,
      environment: { PATH: "/usr/bin" },
      mcpSession,
      extensionPath: "/tmp/cache/draht-t3-mcp-extension.ts",
      runtimeMode: "approval-required",
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--extension",
      "/home/user/.draht/agent/extensions/demo.ts",
      "--session-dir",
      "/tmp/draht-sessions",
      "--provider",
      "anthropic",
      "--model",
      "claude-sonnet",
      "--tools",
      "",
      "--name",
      "-review",
      "--extension-flag=kept",
      "--extension",
      "/tmp/cache/draht-t3-mcp-extension.ts",
    ]);
    assert.notInclude(launch.args, "--no-extensions");
    assert.equal(launch.env[T3_MCP_URL_ENV], "http://127.0.0.1:43123/mcp");
    assert.equal(launch.env[T3_MCP_BEARER_ENV], "secret-draht-token");
    assert.equal(launch.env[T3_DRAHT_RUNTIME_MODE_ENV], "approval-required");
    assert.equal(launch.env["DRAHT_PERMISSION_MODE"], "ask");

    const permissionOnly = buildDrahtRpcLaunch({
      launchArgs: [],
      environment: {
        [T3_MCP_URL_ENV]: "http://127.0.0.1:9999/stale",
        [T3_MCP_BEARER_ENV]: "stale-token",
        DRAHT_PERMISSION_MODE: "yolo",
      },
      mcpSession: undefined,
      extensionPath: "/tmp/cache/draht-t3-mcp-extension.ts",
      runtimeMode: "auto-accept-edits",
    });
    assert.deepEqual(permissionOnly.args, [
      "--mode",
      "rpc",
      "--extension",
      "/tmp/cache/draht-t3-mcp-extension.ts",
    ]);
    assert.isFalse(permissionOnly.hasT3Mcp);
    assert.isUndefined(permissionOnly.env[T3_MCP_URL_ENV]);
    assert.isUndefined(permissionOnly.env[T3_MCP_BEARER_ENV]);
    assert.equal(permissionOnly.env[T3_DRAHT_RUNTIME_MODE_ENV], "auto-accept-edits");
    assert.equal(permissionOnly.env["DRAHT_PERMISSION_MODE"], "ask");
  });

  it("enforces each runtime mode through Draht's own permission mode", () => {
    const permissionMode = (
      runtimeMode: "approval-required" | "auto-accept-edits" | "auto" | "full-access",
    ) =>
      buildDrahtRpcLaunch({
        launchArgs: [],
        environment: {},
        mcpSession: undefined,
        extensionPath: "/tmp/cache/draht-t3-mcp-extension.ts",
        runtimeMode,
      }).env["DRAHT_PERMISSION_MODE"];
    assert.equal(permissionMode("approval-required"), "ask");
    assert.equal(permissionMode("auto-accept-edits"), "ask");
    assert.equal(permissionMode("auto"), "auto");
    assert.equal(permissionMode("full-access"), "yolo");
  });

  it("skips MCP servers only when asked, after every user argument", () => {
    const launch = buildDrahtRpcLaunch({
      launchArgs: ["--context-root", "/repo"],
      environment: {},
      mcpSession: undefined,
      extensionPath: undefined,
      ephemeral: true,
      disableMcp: true,
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--no-session",
      "--context-root",
      "/repo",
      "--no-mcp",
    ]);
    assert.deepInclude(resolveDrahtLaunchArgs("--attach session-1"), { ok: false });
    assert.isTrue(resolveDrahtLaunchArgs("--no-mcp --context-root /repo --duet a/b,c/d").ok);
  });

  it("forces tools and user extensions off for unattended text generation", () => {
    const launch = buildDrahtRpcLaunch({
      launchArgs: [
        "--tools",
        "read,write",
        "--extension",
        "/home/user/.draht/agent/extensions/demo.ts",
        "--extension=./second.ts",
        "--provider",
        "anthropic",
      ],
      environment: {},
      mcpSession,
      extensionPath: "/tmp/cache/draht-t3-mcp-extension.ts",
      sessionPath: "/fake/persisted.jsonl",
      ephemeral: true,
      disableExtensions: true,
      disableTools: true,
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--no-session",
      "--provider",
      "anthropic",
      "--no-extensions",
      "--no-tools",
    ]);
    assert.isFalse(launch.hasT3Mcp);
    assert.deepInclude(resolveDrahtLaunchArgs("--mode text"), {
      ok: false,
      message: "Draht launch argument '--mode' is controlled by T3 Code and cannot be overridden.",
    });
    assert.deepInclude(resolveDrahtLaunchArgs("--session old.jsonl"), { ok: false });
    assert.deepInclude(resolveDrahtLaunchArgs("prompt draht immediately"), { ok: false });
    assert.deepInclude(resolveDrahtLaunchArgs("--plan @instructions.md"), { ok: false });
  });

  it("rejects --provider without --model, which Draht refuses at startup", () => {
    const rejection = {
      ok: false,
      message: "Draht launch argument '--provider' requires '--model'.",
    };
    assert.deepInclude(resolveDrahtLaunchArgs("--provider openrouter"), rejection);
    assert.deepInclude(resolveDrahtLaunchArgs("--provider=openrouter --models gpt-6"), rejection);
    assert.isTrue(resolveDrahtLaunchArgs("--provider openrouter --model=deepseek/v4").ok);
    assert.isTrue(resolveDrahtLaunchArgs("--model deepseek/v4").ok);
  });
});
