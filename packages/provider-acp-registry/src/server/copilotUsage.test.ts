import { describe, expect, it } from "@effect/vitest";

import { copilotUsageFormat, parseCopilotLine } from "./copilotUsage.ts";

// Abbreviated `session.usage_record` events from Copilot CLI 1.0.109 events.jsonl.
const usageRecord = (usage: Record<string, unknown>) =>
  JSON.stringify({
    type: "session.usage_record",
    data: {
      usage: {
        provider: { id: "copilot", kind: "copilot", label: "GitHub Copilot" },
        cost: 0.33,
        initiator: "user",
        ...usage,
      },
    },
    id: "00000000-0000-4000-8000-000000000001",
    timestamp: "2026-11-12T03:22:03.408Z",
  });

describe("parseCopilotLine", () => {
  it("splits cache traffic out of the inclusive input count", () => {
    expect(
      parseCopilotLine(
        usageRecord({
          model: "claude-haiku-4.5",
          inputTokens: 17003,
          outputTokens: 48,
          cacheReadTokens: 16949,
          cacheWriteTokens: 44,
          reasoningTokens: 41,
          accounting: { sourceSessionId: "session-1", sequence: 2, usageId: "msg_2" },
        }),
      ),
    ).toEqual([
      {
        provider: "copilot",
        timestampMs: Date.parse("2026-11-12T03:22:03.408Z"),
        model: "claude-haiku-4.5",
        rateModel: "claude-haiku-4-5",
        sessionId: "session-1",
        totals: {
          uncachedInputTokens: 10,
          cachedInputTokens: 16949,
          cacheCreationTokens: 44,
          outputTokens: 48,
          reasoningTokens: 41,
        },
        reportedCostUsd: null,
        speed: "standard",
        dedupeKey: "copilot:msg_2",
      },
    ]);
  });

  it("counts compaction calls, which have no usage record of their own", () => {
    const line = JSON.stringify({
      type: "session.compaction_complete",
      data: {
        success: true,
        summaryContent: "<overview>...</overview>",
        compactionTokensUsed: {
          requestCount: 1,
          model: "gemini-4-argon",
          inputTokens: 155490,
          outputTokens: 6132,
          cacheReadTokens: 153815,
          cacheWriteTokens: 1671,
        },
      },
      id: "00000000-0000-4000-8000-000000000002",
      timestamp: "2026-10-16T15:00:35.000Z",
    });
    expect(parseCopilotLine(line)).toMatchObject([
      {
        model: "gemini-4-argon",
        sessionId: "",
        totals: {
          uncachedInputTokens: 4,
          cachedInputTokens: 153815,
          cacheCreationTokens: 1671,
          outputTokens: 6132,
        },
        dedupeKey: "copilot:compaction:00000000-0000-4000-8000-000000000002",
      },
    ]);
  });

  it("ignores other events and records without tokens", () => {
    expect(
      parseCopilotLine(JSON.stringify({ type: "assistant.message", data: { model: "gpt-6" } })),
    ).toEqual([]);
    expect(
      parseCopilotLine(usageRecord({ model: "gpt-6.1-sol", inputTokens: 0, outputTokens: 0 })),
    ).toEqual([]);
    expect(parseCopilotLine("{")).toEqual([]);
  });

  it("parses the projected fields the transcript scanner selects", () => {
    const [record] = copilotUsageFormat.parseProjected(
      {
        type: "session.usage_record",
        timestamp: "2026-11-12T03:22:02.266Z",
        data: { usage: { model: "gpt-6.1-sol", inputTokens: 20, outputTokens: 5 } },
      },
      undefined,
    );
    expect(record).toMatchObject({
      model: "gpt-6.1-sol",
      sessionId: "",
      dedupeKey: null,
      totals: { uncachedInputTokens: 20, outputTokens: 5 },
    });
    expect(record).not.toHaveProperty("rateModel");
  });
});
