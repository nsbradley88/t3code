import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/compat";

import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { probeModelOptionDescriptors } from "./probe.ts";

// Shapes from Copilot CLI 1.0.109 session/new and session/set_config_option replies, trimmed.
const modelOption = (currentValue: string): EffectAcpSchema.SessionConfigOption => ({
  type: "select",
  id: "model",
  name: "Model",
  category: "model",
  currentValue,
  options: [
    { value: "auto", name: "Auto" },
    { value: "auto", name: "Auto" },
    { value: "claude-sonnet-5.5", name: "Claude Sonnet 5.5" },
    { value: "gpt-6-luna", name: "GPT-6 Luna" },
    { value: "disabled-model", name: "Disabled" },
  ],
});
const effortOption = (values: ReadonlyArray<string>): EffectAcpSchema.SessionConfigOption => ({
  type: "select",
  id: "reasoning_effort",
  name: "Reasoning Effort",
  category: "thought_level",
  currentValue: "medium",
  options: values.map((value) => ({ value, name: value })),
});
const effortByModel: Record<string, ReadonlyArray<string> | undefined> = {
  auto: undefined,
  "gpt-6-luna": ["none", "low", "medium", "high"],
};

describe("probeModelOptionDescriptors", () => {
  it.effect("reads each advertised model's options from the disposable session", () =>
    Effect.gen(function* () {
      const switched: Array<string> = [];
      const runtime = {
        setConfigOption: (configId: string, value: string | boolean) => {
          switched.push(`${configId}=${String(value)}`);
          if (value === "disabled-model") {
            return Effect.fail(
              new EffectAcpErrors.AcpRequestError({ code: -32602, errorMessage: "disabled" }),
            );
          }
          const effort = effortByModel[String(value)];
          return Effect.succeed({
            configOptions: [modelOption(String(value)), ...(effort ? [effortOption(effort)] : [])],
          });
        },
      } as unknown as AcpSessionRuntime.AcpSessionRuntime["Service"];

      const descriptors = yield* probeModelOptionDescriptors(runtime, {
        sessionId: "probe",
        configOptions: [
          modelOption("claude-sonnet-5.5"),
          effortOption(["low", "medium", "high", "xhigh", "max"]),
        ],
      });

      expect(switched).toEqual(["model=auto", "model=gpt-6-luna", "model=disabled-model"]);
      const effortIds = Object.fromEntries(
        Array.from(descriptors ?? [], ([model, options]) => [
          model,
          options.flatMap((option) =>
            option.type === "select" ? option.options.map((choice) => choice.id) : [],
          ),
        ]),
      );
      expect(effortIds).toEqual({
        "claude-sonnet-5.5": ["low", "medium", "high", "xhigh", "max"],
        auto: [],
        "gpt-6-luna": ["none", "low", "medium", "high"],
      });
    }),
  );
});
