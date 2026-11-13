import { type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";
import { normalizeCliError } from "@t3tools/provider-core/server/textGenerationUtils";
import type { AcpRegistrySettings } from "../settings.ts";
import * as AcpRegistrySupport from "./AcpRegistrySupport.ts";

const COPILOT_TIMEOUT_MS = 180_000;
const MAX_STDERR_BYTES = 8 * 1_024;
const MAX_ERROR_DETAIL_LENGTH = 1_000;
/** `copilot --reasoning-effort` values; Copilot rejects levels a model does not support. */
const COPILOT_REASONING_EFFORTS: ReadonlySet<string> = new Set([
  "none",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);

/**
 * One-shot prompt mode reads the prompt from stdin, which avoids argv limits for large diffs.
 * Copilot has no flag to skip session persistence, so these runs are saved as sessions in the
 * throwaway directory; that keeps them out of project session lists, and sync stays off.
 */
const COPILOT_TEXT_GENERATION_ARGS = [
  "--output-format",
  "json",
  "--no-color",
  "--no-custom-instructions",
  "--disable-builtin-mcps",
  "--no-ask-user",
  "--sync-session=off",
  // Text generation needs no tools. An empty allowlist exposes every tool, so allow only
  // `view` and then exclude it as well: prompts carry diffs, which must not be able to steer
  // the model into reading files (saved trusted folders can widen path access) into output.
  "--available-tools=view",
  "--excluded-tools=view",
  "--disallow-temp-dir",
  // Deny rules outrank every allow rule, including saved approvals and --allow-all.
  "--deny-tool=shell",
  "--deny-tool=write",
  "--deny-tool=memory",
  "--deny-tool=url",
  "--deny-tool=view",
] as const;

const CopilotAssistantMessage = Schema.Struct({
  type: Schema.Literal("assistant.message"),
  data: Schema.Struct({ content: Schema.String }),
});
const decodeAssistantMessage = Schema.decodeUnknownOption(
  Schema.fromJsonString(CopilotAssistantMessage),
);

const isTextGenerationError = Schema.is(TextGenerationError);

function boundedDetail(value: string): string {
  const trimmed = value.trim();
  return trimmed.length <= MAX_ERROR_DETAIL_LENGTH
    ? trimmed
    : `${trimmed.slice(0, MAX_ERROR_DETAIL_LENGTH)}...`;
}

function copilotTextGenerationArgs(modelSelection: ModelSelection): ReadonlyArray<string> {
  const effort = getModelSelectionStringOptionValue(modelSelection, "reasoning_effort");
  return [
    ...COPILOT_TEXT_GENERATION_ARGS,
    // "default" is the placeholder slug used when Copilot advertised no models.
    ...(modelSelection.model === "default" ? [] : ["--model", modelSelection.model]),
    ...(effort !== undefined && COPILOT_REASONING_EFFORTS.has(effort)
      ? ["--reasoning-effort", effort]
      : []),
  ];
}

export const makeCopilotTextGeneration = Effect.fn("makeCopilotTextGeneration")(function* (
  settings: AcpRegistrySettings,
  environment: NodeJS.ProcessEnv,
) {
  const catalog = yield* AcpRegistrySupport.AcpRegistryCatalog;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;

  const runCopilotJson: TextGenerationOperations.Runner = Effect.fn(
    "CopilotTextGeneration.runCopilotJson",
  )(
    function* (request) {
      const { operation, prompt, modelSelection } = request;
      // The prompt carries all context; an empty directory keeps repository instructions,
      // hooks and saved approvals out of the run.
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3code-copilot-text-" });
      const resolved = yield* catalog.resolve(settings, cwd, environment).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: `Could not start GitHub Copilot CLI: ${cause.detail}`,
              cause,
            }),
        ),
      );
      const child = yield* spawner.spawn(
        ChildProcess.make(resolved.spawn.command, copilotTextGenerationArgs(modelSelection), {
          cwd,
          ...(resolved.spawn.env === undefined ? {} : { env: resolved.spawn.env }),
          shell: false,
          stdin: { stream: Stream.encodeText(Stream.make(prompt)) },
          forceKillAfter: "1 second",
        }),
      );
      const [reply, stderr, exitCode] = yield* Effect.all(
        [
          // JSONL streams every delta; keep only the last complete assistant message.
          child.stdout.pipe(
            Stream.decodeText(),
            Stream.splitLines,
            Stream.runFold(
              () => "",
              (last, line) =>
                Option.match(decodeAssistantMessage(line), {
                  onNone: () => last,
                  onSome: ({ data }) => (data.content.trim().length > 0 ? data.content : last),
                }),
            ),
          ),
          collectUint8StreamText({ stream: child.stderr, maxBytes: MAX_STDERR_BYTES }),
          child.exitCode,
        ],
        { concurrency: "unbounded" },
      );
      if (Number(exitCode) !== 0) {
        const detail = boundedDetail(stderr.text);
        return yield* new TextGenerationError({
          operation,
          detail:
            detail.length > 0
              ? `GitHub Copilot CLI command failed: ${detail}`
              : `GitHub Copilot CLI command failed with code ${Number(exitCode)}.`,
        });
      }
      if (reply.trim().length === 0) {
        return yield* new TextGenerationError({
          operation,
          detail: "GitHub Copilot returned empty output.",
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(request, "GitHub Copilot", reply);
    },
    (effect, request) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : normalizeCliError(
                "copilot",
                request.operation,
                cause,
                "GitHub Copilot CLI text generation failed.",
              ),
        ),
        Effect.scoped,
        Effect.timeoutOption(COPILOT_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new TextGenerationError({
                  operation: request.operation,
                  detail: "GitHub Copilot CLI request timed out.",
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      ),
  );

  return TextGenerationOperations.fromRunner(
    "CopilotTextGeneration",
    TextGenerationOperations.withoutInvestigation("GitHub Copilot", runCopilotJson),
  );
});
