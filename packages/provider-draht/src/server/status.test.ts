import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { checkDrahtProviderStatus, MINIMUM_DRAHT_VERSION } from "./status.ts";

const encoder = new TextEncoder();
const decodeRpcRequest = Schema.decodeSync(
  Schema.fromJsonString(Schema.Struct({ id: Schema.String, type: Schema.String })),
);
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function processHandle(input: {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number;
}) {
  const bytes = (value: string | undefined) =>
    value === undefined || value.length === 0
      ? Stream.empty
      : Stream.succeed(encoder.encode(value));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_001),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: bytes(input.stdout),
    stderr: bytes(input.stderr),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function drahtProbeSpawner(version: string) {
  return ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args : [];
    return Effect.succeed(
      args.includes("--version")
        ? processHandle({ stdout: `${version}\n` })
        : processHandle({ stderr: "RPC startup failed", exitCode: 1 }),
    );
  });
}

const drahtDiscoverySpawner = (
  inventory: unknown,
  version = "2026.10.16-1",
  rpcArgs: Array<ReadonlyArray<string>> = [],
) =>
  Effect.gen(function* () {
    const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    let stdinBuffer = "";
    const stdin = Sink.forEach((chunk: Uint8Array) =>
      Effect.gen(function* () {
        stdinBuffer += new TextDecoder().decode(chunk);
        while (true) {
          const newline = stdinBuffer.indexOf("\n");
          if (newline === -1) return;
          const line = stdinBuffer.slice(0, newline);
          stdinBuffer = stdinBuffer.slice(newline + 1);
          if (line.length === 0) continue;
          const request = decodeRpcRequest(line);
          const data =
            request.type === "get_available_models"
              ? inventory
              : request.type === "get_commands"
                ? { commands: [] }
                : {};
          yield* Queue.offer(
            stdout,
            encoder.encode(
              `${encodeJsonLine({ type: "response", id: request.id, command: request.type, success: true, data })}\n`,
            ),
          );
        }
      }),
    );
    return ChildProcessSpawner.make((command) => {
      const args = ChildProcess.isStandardCommand(command) ? command.args : [];
      if (!args.includes("--version")) rpcArgs.push(args);
      return Effect.succeed(
        args.includes("--version")
          ? processHandle({ stdout: `${version}\n` })
          : ChildProcessSpawner.makeHandle({
              ...processHandle({}),
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              stdin,
              stdout: Stream.fromQueue(stdout),
            }),
      );
    });
  });

const settings = {
  enabled: true,
  binaryPath: "draht",
  launchArgs: "",
  customModels: [],
} as const;

describe("DrahtProvider", () => {
  it.effect("requires the oldest Draht release the transcripts were recorded against", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkDrahtProviderStatus(settings).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          drahtProbeSpawner("2026.10.12-1"),
        ),
      );
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.version, "2026.10.12");
      assert.include(snapshot.message ?? "", `Draht ${MINIMUM_DRAHT_VERSION} or newer`);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("accepts a linked source checkout, which reports dev", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkDrahtProviderStatus(settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, drahtProbeSpawner("dev")),
      );
      assert.equal(snapshot.status, "ready");
      assert.isNull(snapshot.version);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    ["2026.10.16-1", false],
    ["2026.10.23-1", true],
  ] as const)(
    "skips MCP servers during discovery of Draht %s only when it can: %s",
    ([version, skips]) =>
      Effect.gen(function* () {
        const rpcArgs: Array<ReadonlyArray<string>> = [];
        const spawner = yield* drahtDiscoverySpawner({ models: [] }, version, rpcArgs);
        yield* checkDrahtProviderStatus(settings).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        assert.lengthOf(rpcArgs, 1);
        assert.equal(rpcArgs[0]?.includes("--no-mcp"), skips);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps compatible Draht selectable when optional discovery fails", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkDrahtProviderStatus(settings).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          drahtProbeSpawner("2026.10.16-1"),
        ),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
      assert.include(snapshot.message ?? "", "could not refresh its models and commands");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports a known unauthenticated inventory when Draht returns no usable models", () =>
    Effect.gen(function* () {
      const spawner = yield* drahtDiscoverySpawner({ models: [] });
      const snapshot = yield* checkDrahtProviderStatus(settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.auth.status, "unauthenticated");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  // regression: #18498 a malformed model inventory must not restore stale Draht models
  it.effect.each([
    ["missing models", {}],
    ["missing model id", { models: [{ provider: "anthropic" }] }],
    [
      "mixed valid and invalid models",
      { models: [{ provider: "anthropic", id: "claude-sonnet-4" }, { provider: "openrouter" }] },
    ],
  ] as const)(
    "reports unknown authentication when Draht discovery returns %s",
    ([_label, inventory]) =>
      Effect.gen(function* () {
        const spawner = yield* drahtDiscoverySpawner(inventory);
        const snapshot = yield* checkDrahtProviderStatus(settings).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        assert.equal(snapshot.status, "ready");
        assert.equal(snapshot.auth.status, "unknown");
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});
