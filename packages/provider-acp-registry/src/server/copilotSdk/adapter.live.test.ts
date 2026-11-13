// Live smoke test against a real `copilot` binary: T3_COPILOT_LIVE=1 (spends premium requests).
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Stream from "effect/Stream";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { makeCopilotSdkAdapterV2 } from "./adapter.ts";

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  SecureRandom.layerWebCrypto,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const INSTANCE_ID = ProviderInstanceId.make("copilot_live");
const THREAD_ID = ThreadId.make("thread-copilot-live");
const MODEL = process.env.T3_COPILOT_LIVE_MODEL ?? "default";
const CWD = process.env.T3_COPILOT_LIVE_CWD ?? "/tmp/t3-copilot-live";
const selection = { instanceId: INSTANCE_ID, model: MODEL, options: [] };
const policy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: CWD,
});

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  ordinal: number,
  text: string,
): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2TurnInput> {
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: THREAD_ID,
    projectId: ProjectId.make("project-live"),
    title: "Live",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: INSTANCE_ID,
    modelSelection: selection,
    runtimeMode: policy.runtimeMode,
    interactionMode: policy.interactionMode,
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  return {
    appThread,
    threadId: THREAD_ID,
    runId: RunId.make(`run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt-${ordinal}`),
    rootNodeId: NodeId.make(`root-${ordinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message-${ordinal}`),
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection,
    runtimePolicy: policy,
  };
});

const SLOW = "Run `sleep 60; echo finished` as ONE shell command and wait for it. Do nothing else.";

describe.skipIf(process.env.T3_COPILOT_LIVE !== "1")("Copilot SDK adapter (live)", () => {
  it.live(
    "steers a running tool within seconds, reports per-turn usage, interrupts, and stops",
    () =>
      Effect.gen(function* () {
        const adapter = yield* makeCopilotSdkAdapterV2({
          instanceId: INSTANCE_ID,
          resolveSpawn: (cwd) =>
            Effect.succeed({
              command: process.env.COPILOT_BIN ?? "copilot",
              args: [],
              env: process.env,
              cwd,
            }),
          requestTimeoutMs: 60_000,
        });
        const runtime = yield* adapter.openSession({
          threadId: THREAD_ID,
          providerSessionId: ProviderSessionId.make("session-copilot-live"),
          modelSelection: selection,
          runtimePolicy: policy,
        });
        const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const allEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => allEvents.push(event)).pipe(
              Effect.andThen(Queue.offer(emitted, event)),
            ),
          ),
          Effect.ignore,
          Effect.forkScoped,
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: selection,
          runtimePolicy: policy,
        });
        const elapsed = Effect.map(DateTime.now, DateTime.toEpochMillis);
        const log: string[] = [];
        const t0 = yield* elapsed;
        const waitFor = <E extends ProviderAdapter.ProviderAdapterV2Event>(
          label: string,
          predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => event is E,
          ms = 120_000,
        ) =>
          Effect.gen(function* () {
            while (true) {
              const event = yield* Queue.take(emitted);
              log.push(
                `+${(yield* elapsed) - t0}ms ${event.type === "turn_item.updated" ? `${event.turnItem.type}:${event.turnItem.status}` : event.type}`,
              );
              if (predicate(event)) return event;
            }
          }).pipe(
            Effect.timeoutOrElse({
              duration: ms,
              orElse: () =>
                Effect.die(
                  new Error(`timed out waiting for ${label}: ${log.slice(-25).join(" > ")}`),
                ),
            }),
          );

        const first = yield* turnInput(providerThread, 1, SLOW);
        yield* runtime.startTurn(first);
        const running = yield* waitFor(
          "provider turn",
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        yield* waitFor(
          "running shell command",
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "running",
        );
        const steeredAt = yield* elapsed;
        yield* runtime.steerTurn({
          threadId: THREAD_ID,
          runId: first.runId,
          providerThread,
          providerTurnId: running.providerTurn.id,
          message: {
            ...first.message,
            text: "Stop waiting. Reply with exactly the single word PIVOT.",
          },
        });
        yield* waitFor(
          "PIVOT reply",
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "message.updated" }
          > =>
            event.type === "message.updated" &&
            event.message.role === "assistant" &&
            /PIVOT/.test(event.message.text),
        );
        const replyMs = (yield* elapsed) - steeredAt;
        const terminal = yield* waitFor(
          "turn 1 terminal",
          (
            event,
          ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        );
        assert.strictEqual(terminal.status, "completed");
        yield* Effect.log(
          `steer -> PIVOT reply: ${replyMs}ms (the tool call was still asleep for ~60s)`,
        );
        assert.ok(replyMs < 20_000, `steer took ${replyMs}ms`);

        const firstDone = allEvents.findLast(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.id === running.providerTurn.id &&
            event.providerTurn.status === "completed",
        );
        assert.ok(firstDone?.type === "provider_turn.updated");
        const firstUsage = firstDone.providerTurn.turnTokenUsage;
        yield* Effect.log(`turn 1 usage: ${JSON.stringify(firstUsage)}`);
        assert.ok((firstUsage?.inputTokens ?? 0) > 0 && (firstUsage?.outputTokens ?? 0) > 0);

        const second = yield* turnInput(providerThread, 2, SLOW);
        yield* runtime.startTurn(second);
        const running2 = yield* waitFor(
          "second turn",
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.status === "running" &&
            event.providerTurn.ordinal === 2,
        );
        yield* waitFor(
          "second shell command",
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "running",
        );
        const interruptedAt = yield* elapsed;
        yield* runtime.interruptTurn({ providerThread, providerTurnId: running2.providerTurn.id });
        const second_terminal = yield* waitFor(
          "turn 2 terminal",
          (
            event,
          ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
          30_000,
        );
        const interruptMs = (yield* elapsed) - interruptedAt;
        yield* Effect.log(`interrupt -> terminal: ${interruptMs}ms (${second_terminal.status})`);
        assert.strictEqual(second_terminal.status, "interrupted");
        assert.ok(interruptMs < 20_000, `interrupt took ${interruptMs}ms`);

        const secondDone = allEvents.findLast(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.id === running2.providerTurn.id &&
            event.providerTurn.status === "interrupted",
        );
        assert.ok(secondDone?.type === "provider_turn.updated");
        yield* Effect.log(`timeline: ${log.join(" | ")}`);
        yield* Effect.log(
          `turn 2 usage: ${JSON.stringify(secondDone.providerTurn.turnTokenUsage)}`,
        );
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    300_000,
  );
});
