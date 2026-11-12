import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import {
  parseTimestampMs,
  tokenCount,
  totalTokens,
  type ProviderUsageReader,
  type TranscriptUsageFormat,
  type UsageRecord,
} from "@t3tools/provider-core/server/usage";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";

import type { CopilotSettings } from "../settings.ts";

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null ? (value as Record<string, unknown>) : undefined;

/**
 * Copilot CLI logs one `session.usage_record` per model call, subagent calls included.
 * Compaction calls are logged only on their `session.compaction_complete` event.
 */
function parseCopilotRecord(parsed: unknown): readonly UsageRecord[] {
  const record = asRecord(parsed);
  const data = asRecord(record?.["data"]);
  const compaction = record?.["type"] === "session.compaction_complete";
  if (!compaction && record?.["type"] !== "session.usage_record") return [];
  const usage = asRecord(compaction ? data?.["compactionTokensUsed"] : data?.["usage"]);
  if (record === undefined || usage === undefined) return [];
  const timestampMs = parseTimestampMs(record["timestamp"]);
  const model = typeof usage["model"] === "string" ? usage["model"].trim() : "";
  if (timestampMs === null || model.length === 0) return [];

  // inputTokens already counts cache reads and writes.
  const input = tokenCount(usage["inputTokens"]);
  const cachedInputTokens = tokenCount(usage["cacheReadTokens"]);
  const cacheCreationTokens = tokenCount(usage["cacheWriteTokens"]);
  const outputTokens = tokenCount(usage["outputTokens"]);
  const totals = {
    uncachedInputTokens: Math.max(0, input - cachedInputTokens - cacheCreationTokens),
    cachedInputTokens,
    cacheCreationTokens,
    outputTokens,
    reasoningTokens: Math.min(outputTokens, tokenCount(usage["reasoningTokens"])),
  };
  if (totalTokens(totals) === 0) return [];

  const accounting = asRecord(usage["accounting"]);
  const sessionId = accounting?.["sourceSessionId"];
  const usageId = compaction ? record["id"] : accounting?.["usageId"];
  // Copilot spells Claude versions with dots; the rate table uses dashes.
  const rateModel = model.startsWith("claude-") ? model.replaceAll(".", "-") : model;
  return [
    {
      provider: "copilot",
      timestampMs,
      model,
      ...(rateModel === model ? {} : { rateModel }),
      sessionId: typeof sessionId === "string" ? sessionId : "",
      totals,
      reportedCostUsd: null,
      speed: "standard",
      dedupeKey:
        typeof usageId === "string" && usageId
          ? `copilot:${compaction ? "compaction:" : ""}${usageId}`
          : null,
    },
  ];
}

const USAGE_FIELDS = {
  model: true,
  inputTokens: true,
  outputTokens: true,
  cacheReadTokens: true,
  cacheWriteTokens: true,
  reasoningTokens: true,
  accounting: true,
} as const;

export function parseCopilotLine(line: string): readonly UsageRecord[] {
  try {
    return parseCopilotRecord(JSON.parse(line));
  } catch {
    return [];
  }
}

export const copilotUsageFormat: TranscriptUsageFormat<void> = {
  selectFields: {
    type: true,
    id: true,
    timestamp: true,
    data: { usage: USAGE_FIELDS, compactionTokensUsed: USAGE_FIELDS },
  },
  usageMarkers: ['"session.usage_record"', '"session.compaction_complete"'],
  parseLine: (line) => parseCopilotLine(line),
  parseProjected: (projected) => parseCopilotRecord(projected),
};

export const copilotUsageReader: ProviderUsageReader<CopilotSettings, Path.Path> = {
  kind: "transcripts",
  provider: "copilot",
  format: copilotUsageFormat,
  directories: Effect.fn("copilotUsageReader.directories")(function* ({ environment }) {
    const path = yield* Path.Path;
    const homeDirectory = yield* HostProcess.HomeDirectory;
    const home = expandHomePath(
      environment.COPILOT_HOME?.trim() || path.join(homeDirectory, ".copilot"),
      homeDirectory,
    );
    return [{ dir: path.resolve(home, "session-state"), fileName: "events.jsonl" }];
  }),
};
