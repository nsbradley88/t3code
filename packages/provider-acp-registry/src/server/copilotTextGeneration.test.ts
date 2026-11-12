import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { createModelSelection } from "@t3tools/shared/model";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { CopilotSettings } from "../settings.ts";
import * as AcpRegistrySupport from "./AcpRegistrySupport.ts";
import { makeCopilotTextGeneration } from "./copilotTextGeneration.ts";

const encoder = new TextEncoder();
const settings = Schema.decodeSync(CopilotSettings)({ enabled: true, commandPath: "copilot" });
const environment = { PATH: "/provider/bin", COPILOT_GH_HOST: "example.ghe.com" };
const instanceId = ProviderInstanceId.make("copilot");

// Abridged `copilot --output-format json` stream from Copilot CLI 1.0.109.
const jsonl = (content: string) =>
  [
    { type: "session.start", data: { sessionId: "session-1", copilotVersion: "1.0.109" } },
    { type: "assistant.message_delta", data: { messageId: "m1", deltaContent: "```json" } },
    { type: "assistant.message", data: { messageId: "m1", content, toolRequests: [] } },
    { type: "assistant.turn_end", data: { turnId: "0" } },
    { type: "result", sessionId: "session-1", exitCode: 0 },
  ]
    .map((event) => JSON.stringify(event))
    .join("\n");

interface Spawned {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: unknown;
  readonly stdin: string;
}

function stdinText(stdin: ChildProcess.CommandOptions["stdin"]) {
  const input = typeof stdin === "object" && "stream" in stdin ? stdin.stream : stdin;
  return typeof input === "object"
    ? input.pipe(Stream.decodeText(), Stream.mkString)
    : Effect.succeed("");
}

function fakeCopilot(output: { stdout: string; stderr?: string; exitCode?: number }) {
  const spawned: Array<Spawned> = [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) return yield* Effect.die("unexpected pipe");
      const stdin = yield* stdinText(command.options.stdin);
      spawned.push({
        command: command.command,
        args: command.args.map(String),
        cwd: command.options.cwd,
        env: command.options.env,
        stdin,
      });
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(900_000_201),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(output.exitCode ?? 0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.succeed(encoder.encode(output.stdout)),
        stderr: output.stderr ? Stream.succeed(encoder.encode(output.stderr)) : Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }).pipe(Effect.orDie),
  );
  const catalog: AcpRegistrySupport.AcpRegistryCatalog["Service"] = {
    search: () => Effect.die("unused search"),
    prepare: () => Effect.die("unused prepare"),
    inspect: () => Effect.die("unused inspect"),
    resolve: (_settings, cwd, env) =>
      Effect.succeed({
        distribution: "local",
        spawn: {
          command: "/provider/bin/copilot",
          args: ["--acp"],
          cwd,
          ...(env === undefined ? {} : { env }),
          shell: false,
        },
      }),
    uninstallManagedBinary: () => Effect.die("unused uninstall"),
  };
  const textGeneration = makeCopilotTextGeneration(settings, environment).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, catalog),
  );
  return { spawned, textGeneration };
}

describe("CopilotTextGeneration", () => {
  it.effect("runs a tool-restricted one-shot prompt outside the workspace", () =>
    Effect.gen(function* () {
      const fake = fakeCopilot({
        stdout: jsonl('```json\n{"subject":"Export answer constant.","body":"- adds it"}\n```'),
      });
      const textGeneration = yield* fake.textGeneration;
      const generated = yield* textGeneration.generateCommitMessage({
        cwd: "/workspace/project",
        branch: "main",
        stagedSummary: "M src/answer.ts",
        stagedPatch: "+export const answer = 42;",
        modelSelection: createModelSelection(instanceId, "gpt-6-luna", [
          { id: "reasoning_effort", value: "low" },
        ]),
      });

      expect(generated).toEqual({ subject: "Export answer constant", body: "- adds it" });
      const [run] = fake.spawned;
      expect(run).toMatchObject({ command: "/provider/bin/copilot", env: environment });
      expect(run?.cwd).not.toBe("/workspace/project");
      expect(run?.cwd).toContain("t3code-copilot-text-");
      // Prompts carry diffs, so they travel on stdin rather than argv.
      expect(run?.stdin).toContain("+export const answer = 42;");
      expect(run?.args.join(" ")).not.toContain("answer");
      expect(run?.args).toEqual([
        "--output-format",
        "json",
        "--no-color",
        "--no-custom-instructions",
        "--disable-builtin-mcps",
        "--no-ask-user",
        "--sync-session=off",
        "--available-tools=view",
        "--disallow-temp-dir",
        "--deny-tool=shell",
        "--deny-tool=write",
        "--deny-tool=memory",
        "--deny-tool=url",
        "--model",
        "gpt-6-luna",
        "--reasoning-effort",
        "low",
      ]);
      expect(run?.args).not.toContain("--acp");
      expect(run?.args.some((arg) => arg.startsWith("--allow"))).toBe(false);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("leaves the model and effort to Copilot when none is chosen", () =>
    Effect.gen(function* () {
      const fake = fakeCopilot({ stdout: jsonl('{"title":"Fix mobile login alignment"}') });
      const textGeneration = yield* fake.textGeneration;
      const generated = yield* textGeneration.generateThreadTitle({
        cwd: "/workspace/project",
        message: "fix the login button alignment on mobile",
        modelSelection: createModelSelection(instanceId, "default", [
          { id: "reasoning_effort", value: "turbo" },
        ]),
      });

      expect(generated.title).toBe("Fix mobile login alignment");
      const args = fake.spawned[0]?.args ?? [];
      expect(args).not.toContain("--model");
      expect(args).not.toContain("--reasoning-effort");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    {
      label: "a failed run",
      output: { stdout: "", stderr: "Error: Model 'nope' is not available.\n", exitCode: 1 },
      detail: "GitHub Copilot CLI command failed: Error: Model 'nope' is not available.",
    },
    {
      label: "a run without an assistant message",
      output: { stdout: jsonl("   ") },
      detail: "GitHub Copilot returned empty output.",
    },
    {
      label: "a reply that is not the requested JSON",
      output: { stdout: jsonl("I cannot help with that.") },
      detail: "GitHub Copilot returned invalid structured output.",
    },
  ])("reports $label", ({ output, detail }) =>
    Effect.gen(function* () {
      const textGeneration = yield* fakeCopilot(output).textGeneration;
      const error = yield* textGeneration
        .generatePrContent({
          cwd: "/workspace/project",
          baseBranch: "main",
          headBranch: "feature",
          commitSummary: "abc123 Add answer",
          diffSummary: "M src/answer.ts",
          diffPatch: "+export const answer = 42;",
          modelSelection: createModelSelection(instanceId, "claude-haiku-4.5"),
        })
        .pipe(Effect.flip);
      expect(error).toMatchObject({ _tag: "TextGenerationError", detail });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
