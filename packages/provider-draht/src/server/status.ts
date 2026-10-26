import {
  type CustomModelSetting,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type { DrahtSettings } from "../settings.ts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { buildDrahtRpcLaunch, resolveDrahtLaunchArgs } from "./mcpInjection.ts";
import {
  makeDrahtRpcConnection,
  DrahtRpcError,
  DrahtRpcTimeoutError,
  drahtRecordField as recordField,
  drahtRecordString as recordString,
} from "./rpc.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "@t3tools/provider-core/server/maintenanceResolver";
import {
  EMPTY_DRAHT_MODEL_CAPABILITIES,
  thinkingCapabilitiesForDrahtModel,
} from "./thinkingCapabilities.ts";
import {
  parseDrahtDiscoveredCommands,
  withDrahtBuiltinSlashCommands,
  type DrahtDiscoveredCommands,
} from "./commands.ts";

const DRAHT_PRESENTATION = {
  displayName: "Draht",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "auto", "full-access"],
  reportsContextWindow: true,
  requiresNewThreadForModelChange: false,
} as const;

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const DRAHT_RPC_DISCOVERY_TIMEOUT_MS = 30_000;
/**
 * Draht uses calendar versions (`2026.10.16-1`). This is the oldest release the RPC
 * transcript fixtures were recorded against; it carries pi's `get_entries`,
 * `agent_settled`, and tool exposure APIs.
 */
export const MINIMUM_DRAHT_VERSION = "2026.10.16";

/** The first release that accepts `--no-mcp`; older ones exit on unknown flags. */
const DRAHT_NO_MCP_VERSION = "2026.10.23";

/** Background discovery must not connect the user's MCP servers when Draht can skip them. */
function drahtDiscoveryDisablesMcp(version: string | null | undefined): boolean {
  return (
    version !== null &&
    version !== undefined &&
    compareSemverVersions(version, DRAHT_NO_MCP_VERSION) >= 0
  );
}

/** A linked source checkout reports `dev` instead of a release version. */
function isDrahtDevelopmentBuild(output: string): boolean {
  return output.trim() === "dev";
}

const DRAHT_DEFAULT_MODEL: ServerProviderModel = {
  slug: "default",
  name: "Draht default",
  isCustom: false,
  capabilities: EMPTY_DRAHT_MODEL_CAPABILITIES,
};

interface DrahtDiscovery extends DrahtDiscoveredCommands {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly authenticated: boolean;
}

function drahtModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    [DRAHT_DEFAULT_MODEL, ...discovered],
    customModels ?? [],
    EMPTY_DRAHT_MODEL_CAPABILITIES,
  );
}

function parseDiscoveredModels(
  data: unknown,
  state: unknown,
): Result.Result<ReadonlyArray<ServerProviderModel>, DrahtRpcError> {
  const models = recordField(data, "models");
  if (!Array.isArray(models)) {
    return Result.fail(
      new DrahtRpcError({ operation: "get_available_models", detail: "Invalid model inventory" }),
    );
  }
  const activeModel = recordField(state, "model");
  const activeProvider = recordString(activeModel, "provider");
  const activeModelId = recordString(activeModel, "id");
  const seen = new Set<string>();
  const parsed: Array<ServerProviderModel> = [];
  for (const model of models) {
    const provider = recordString(model, "provider");
    const id = recordString(model, "id");
    if (provider === undefined || id === undefined || provider.trim() === "" || id.trim() === "") {
      return Result.fail(
        new DrahtRpcError({ operation: "get_available_models", detail: "Invalid model identity" }),
      );
    }
    const slug = `${provider}/${id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    parsed.push({
      slug,
      name: recordString(model, "name") ?? slug,
      subProvider: provider,
      isCustom: false,
      capabilities: thinkingCapabilitiesForDrahtModel(
        model,
        provider === activeProvider && id === activeModelId
          ? recordString(state, "thinkingLevel")
          : undefined,
      ),
    });
  }
  return Result.succeed(parsed);
}

const makeDrahtDiscoveryConnection = Effect.fnUntraced(function* (
  drahtSettings: DrahtSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  disableMcp: boolean,
  cwd?: string,
) {
  const launch = buildDrahtRpcLaunch({
    launchArgs,
    environment,
    mcpSession: undefined,
    extensionPath: undefined,
    ephemeral: true,
    disableMcp,
  });
  const connection = yield* makeDrahtRpcConnection({
    command: drahtSettings.binaryPath || "draht",
    args: launch.args,
    cwd,
    env: launch.env,
  });
  yield* Stream.fromQueue(connection.events).pipe(
    Stream.runDrain,
    Effect.ignore,
    Effect.forkScoped,
  );
  return connection;
});

const discoverDrahtViaRpc = Effect.fn("DrahtProviderStatus.discoverDrahtViaRpc")(function* (
  drahtSettings: DrahtSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  disableMcp: boolean,
  cwd?: string,
) {
  const connection = yield* makeDrahtDiscoveryConnection(
    drahtSettings,
    environment,
    launchArgs,
    disableMcp,
    cwd,
  );
  const stateData = yield* connection.request({ type: "get_state" });
  const modelsData = yield* connection.request({ type: "get_available_models" });
  const commandsData = yield* connection
    .request({ type: "get_commands" })
    .pipe(Effect.orElseSucceed(() => undefined));
  const discoveredModels = yield* Effect.fromResult(parseDiscoveredModels(modelsData, stateData));
  const { slashCommands, skills } = parseDrahtDiscoveredCommands(commandsData);
  return {
    models: discoveredModels,
    slashCommands: withDrahtBuiltinSlashCommands(slashCommands),
    skills,
    authenticated: discoveredModels.length > 0,
  } satisfies DrahtDiscovery;
}, Effect.scoped);

export const discoverDrahtCommandsForCwd = Effect.fn("discoverDrahtCommandsForCwd")(
  function* (
    drahtSettings: DrahtSettings,
    environment: NodeJS.ProcessEnv,
    cwd: string,
    version: string | null,
  ) {
    const launchArgs = resolveDrahtLaunchArgs(drahtSettings.launchArgs);
    if (!launchArgs.ok) {
      return yield* new DrahtRpcError({ operation: "launch", detail: launchArgs.message });
    }
    const connection = yield* makeDrahtDiscoveryConnection(
      drahtSettings,
      environment,
      launchArgs.args,
      drahtDiscoveryDisablesMcp(version),
      cwd,
    );
    // A failed read must not replace a previously usable workspace catalog with an empty one.
    const commandsData = yield* connection.request({ type: "get_commands" });
    const { slashCommands, skills } = parseDrahtDiscoveredCommands(commandsData);
    return { slashCommands: withDrahtBuiltinSlashCommands(slashCommands), skills };
  },
  Effect.scoped,
  Effect.timeoutOrElse({
    duration: DRAHT_RPC_DISCOVERY_TIMEOUT_MS,
    orElse: () =>
      Effect.fail(
        new DrahtRpcTimeoutError({
          operation: "discovery",
          timeoutMs: DRAHT_RPC_DISCOVERY_TIMEOUT_MS,
        }),
      ),
  }),
);

const runDrahtVersionCommand = Effect.fn("DrahtProviderStatus.runDrahtVersionCommand")(function* (
  drahtSettings: DrahtSettings,
  environment: NodeJS.ProcessEnv,
) {
  const command = drahtSettings.binaryPath || "draht";
  const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
    env: environment,
  });
  return yield* spawnAndCollect(
    command,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: environment,
      shell: spawnCommand.shell,
    }),
  );
});

export const buildInitialDrahtProviderSnapshot = Effect.fn(
  "DrahtProviderStatus.buildInitialDrahtProviderSnapshot",
)(function* (drahtSettings: DrahtSettings): Effect.fn.Return<ServerProviderDraft> {
  const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
  const models = drahtModelsFromSettings(drahtSettings.customModels);
  if (!drahtSettings.enabled) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Draht is disabled in T3 Code settings.",
      },
    });
  }
  return buildServerProvider({
    presentation: DRAHT_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Checking Draht CLI availability...",
    },
  });
});

export const checkDrahtProviderStatus = Effect.fn("checkDrahtProviderStatus")(function* (
  drahtSettings: DrahtSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = drahtModelsFromSettings(drahtSettings.customModels);

  if (!drahtSettings.enabled) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Draht is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runDrahtVersionCommand(drahtSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Draht CLI health check failed.", { errorTag: error._tag });
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Draht CLI (`draht`) is not installed or not on PATH. Install with `npm install -g @draht/coding-agent`."
          : "Failed to execute Draht CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Draht CLI is installed but timed out while running `draht --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Draht CLI is installed but failed to run.",
      },
    });
  }

  const developmentBuild = version === null && isDrahtDevelopmentBuild(versionOutput.stdout);
  if (version === null && !developmentBuild) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `T3 Code could not determine the Draht version. Draht ${MINIMUM_DRAHT_VERSION} or newer is required.`,
      },
    });
  }

  if (version !== null && compareSemverVersions(version, MINIMUM_DRAHT_VERSION) < 0) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: `Draht ${version} is unsupported. Update to Draht ${MINIMUM_DRAHT_VERSION} or newer.`,
      },
    });
  }

  const resolvedLaunchArgs = resolveDrahtLaunchArgs(drahtSettings.launchArgs);
  if (!resolvedLaunchArgs.ok) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: resolvedLaunchArgs.message,
      },
    });
  }

  const discoveryExit = yield* discoverDrahtViaRpc(
    drahtSettings,
    environment,
    resolvedLaunchArgs.args,
    drahtDiscoveryDisablesMcp(version),
    cwd,
  ).pipe(Effect.timeoutOption(DRAHT_RPC_DISCOVERY_TIMEOUT_MS), Effect.exit);
  if (Exit.isFailure(discoveryExit)) {
    yield* Effect.logWarning("Draht RPC discovery failed.", {
      errorTag: causeErrorTag(discoveryExit.cause),
    });
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
        message:
          "Draht is available, but T3 Code could not refresh its models and commands. The live session will retry startup.",
      },
    });
  }
  if (Option.isNone(discoveryExit.value)) {
    return buildServerProvider({
      presentation: DRAHT_PRESENTATION,
      enabled: drahtSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
        message:
          "Draht is available, but model and command discovery needs interactive input. The live session will handle it.",
      },
    });
  }

  const discovery = discoveryExit.value.value;
  const models = drahtModelsFromSettings(drahtSettings.customModels, discovery.models);
  return buildServerProvider({
    presentation: DRAHT_PRESENTATION,
    enabled: drahtSettings.enabled,
    checkedAt,
    models,
    slashCommands: discovery.slashCommands,
    skills: discovery.skills,
    probe: {
      installed: true,
      version,
      status: discovery.authenticated ? "ready" : "warning",
      auth: {
        status: discovery.authenticated ? "authenticated" : "unauthenticated",
        type: "draht",
      },
      ...(discovery.authenticated
        ? {}
        : {
            message:
              "Draht has no usable models. Run `draht` in a terminal and use /login, or configure an API key in ~/.draht/agent.",
          }),
    },
  });
});

export const enrichDrahtSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<
  void,
  never,
  HttpClient.HttpClient | ProviderLatestVersions.ProviderLatestVersions
> => {
  const { snapshot, publishSnapshot } = input;
  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Draht version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
