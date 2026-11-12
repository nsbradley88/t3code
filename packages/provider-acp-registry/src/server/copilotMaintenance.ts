import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import {
  makePackageManagedProviderMaintenanceResolver,
  normalizeCommandPath,
} from "@t3tools/provider-core/server/maintenanceResolver";
import type { AcpRegistrySettings } from "../settings.ts";
import * as AcpRegistrySupport from "./AcpRegistrySupport.ts";

const VERSION_TIMEOUT = "10 seconds";
const MAX_VERSION_OUTPUT_BYTES = 4 * 1_024;
const COPILOT_VERSION_PATTERN = /\b(\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*)?)/;

/** The official install script's default location for non-root installs. */
function isStandaloneCopilotCommandPath(commandPath: string): boolean {
  return /\/\.local\/bin\/copilot(?:\.exe)?$/.test(normalizeCommandPath(commandPath));
}

// `copilot update` downloads into Copilot's own package cache, which only the standalone
// launcher reads; other installs stay with their package manager or remain manual.
export const COPILOT_MAINTENANCE = makePackageManagedProviderMaintenanceResolver({
  provider: ProviderDriverKind.make("copilot"),
  npmPackageName: "@github/copilot",
  nativeUpdate: {
    args: ["update"],
    isCommandPath: isStandaloneCopilotCommandPath,
    requiresCommandPath: true,
  },
});

/** Parses `GitHub Copilot CLI 1.0.109.` from `copilot --version`. */
export function parseCopilotVersion(output: string): string | null {
  return COPILOT_VERSION_PATTERN.exec(output)?.[1] ?? null;
}

/** Local ACP inspection reports no version, so ask the resolved executable. */
export const probeCopilotVersion = Effect.fn("CopilotDriver.probeVersion")(
  function* (settings: AcpRegistrySettings, cwd: string, environment: NodeJS.ProcessEnv) {
    const catalog = yield* AcpRegistrySupport.AcpRegistryCatalog;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const resolved = yield* catalog.resolve(settings, cwd, environment);
    const child = yield* spawner.spawn(
      ChildProcess.make(resolved.spawn.command, ["--version"], {
        cwd,
        ...(resolved.spawn.env === undefined ? {} : { env: resolved.spawn.env }),
        shell: false,
        forceKillAfter: "1 second",
      }),
    );
    const [stdout, , exitCode] = yield* Effect.all(
      [
        collectUint8StreamText({ stream: child.stdout, maxBytes: MAX_VERSION_OUTPUT_BYTES }),
        Stream.runDrain(child.stderr),
        child.exitCode,
      ],
      { concurrency: "unbounded" },
    );
    return Number(exitCode) === 0 ? parseCopilotVersion(stdout.text) : null;
  },
  (effect) =>
    effect.pipe(
      Effect.scoped,
      Effect.timeoutOption(VERSION_TIMEOUT),
      Effect.map(Option.getOrNull),
      Effect.orElseSucceed(() => null),
    ),
);
