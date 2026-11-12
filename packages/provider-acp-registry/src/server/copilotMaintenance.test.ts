import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { COPILOT_MAINTENANCE, parseCopilotVersion } from "./copilotMaintenance.ts";

const noSpawn = ChildProcessSpawner.make(() =>
  Effect.die("maintenance resolution should not spawn a process here"),
);

describe("Copilot maintenance", () => {
  it.effect.each([
    {
      label: "updates a standalone install with its own updater",
      resolved: "/home/me/.local/bin/copilot",
      real: "/home/me/.local/bin/copilot",
      update: {
        executable: "/home/me/.local/bin/copilot",
        args: ["update"],
        lockKey: "copilot-native",
      },
    },
    {
      label: "updates an npm global install with npm",
      resolved: "/usr/local/bin/copilot",
      real: "/usr/local/lib/node_modules/@github/copilot/npm-loader.js",
      update: {
        executable: "npm",
        args: [
          "install",
          "-g",
          "--prefix",
          "/usr/local",
          "--allow-scripts=@github/copilot",
          "@github/copilot@latest",
        ],
        lockKey: "npm-global:/usr/local",
      },
    },
    {
      label: "prefers npm when an npm prefix shares the standalone bin directory",
      resolved: "/home/me/.local/bin/copilot",
      real: "/home/me/.local/lib/node_modules/@github/copilot/npm-loader.js",
      update: { executable: "npm", lockKey: "npm-global:/home/me/.local" },
    },
    {
      label: "leaves an unrecognized install manual-only",
      resolved: "/opt/copilot/bin/copilot",
      real: "/opt/copilot/bin/copilot",
      update: null,
    },
  ])("$label", ({ resolved, real, update }) =>
    Effect.gen(function* () {
      const capabilities = yield* COPILOT_MAINTENANCE.resolve({
        binaryPath: "copilot",
        resolvedCommandPath: resolved,
        realCommandPath: real,
        env: {},
        platform: "linux",
      }).pipe(
        Effect.provideService(HostProcess.Platform, "linux"),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn),
        Effect.provide(NodeServices.layer),
      );

      expect(capabilities.packageName).toBe("@github/copilot");
      if (update === null) {
        expect(capabilities.update).toBeNull();
        expect(capabilities.installPath).toBe(real);
      } else {
        expect(capabilities.update).toMatchObject(update);
      }
    }),
  );

  it.each([
    ["GitHub Copilot CLI 1.0.109.\nRun 'copilot update' to check for updates.\n", "1.0.109"],
    ["GitHub Copilot CLI 1.0.110-1.\n", "1.0.110-1"],
    ["copilot: command failed\n", null],
  ])("parses %j", (output, version) => {
    expect(parseCopilotVersion(output)).toBe(version);
  });
});
