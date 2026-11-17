// Snapshot, rollback and fork over the journal, and subagent tool rows, against a fake Copilot server.
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
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

import {
  makeCopilotSdkAdapterV2,
  CopilotSdkCapabilitiesV2,
  type CopilotSdkAdapterV2Options,
} from "./adapter.ts";
import type { CopilotSdkCloseInfo, CopilotSdkConnection } from "./connection.ts";
import {
  discardedTurns,
  forkCutoffBefore,
  isTurnStart,
  journalMessages,
  parseJournalEvent,
  readJournal,
  resolveBoundaryEvent,
} from "./threadOps.ts";

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  SecureRandom.layerWebCrypto,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const COPILOT = ProviderDriverKind.make("copilot");
const INSTANCE_ID = ProviderInstanceId.make("copilot_work");
const THREAD_ID = ThreadId.make("thread-copilot-sdk-test");
const MODEL = "gpt-5.4";
const fullAccess = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});
const selection = { instanceId: INSTANCE_ID, model: MODEL, options: [] };

interface Call {
  readonly method: string;
  readonly params: Record<string, unknown>;
}

/** An in-memory stand-in for `copilot --headless --stdio`. */
const makeFakeServer = Effect.fnUntraced(function* () {
  const calls: Call[] = [];
  const requests = yield* Queue.unbounded<Call>();
  const responses = new Map<string, Array<unknown>>();
  const closeHandlers: Array<(info: CopilotSdkCloseInfo) => void> = [];
  const requestHandlers = new Map<string, (params: unknown) => Promise<unknown>>();
  let notify: (method: string, params: unknown) => void = () => {};
  let sessionId = "";
  const connection: CopilotSdkConnection = {
    request: async (method, params = {}) => {
      const call = { method, params };
      calls.push(call);
      Queue.offerUnsafe(requests, call);
      if (method === "session.create" || method === "session.resume")
        sessionId = String(params.sessionId);
      const queued = responses.get(method)?.shift();
      if (queued instanceof Error) throw queued;
      if (queued !== undefined) return queued;
      switch (method) {
        case "session.create":
        case "session.resume":
          return { sessionId };
        case "session.send":
          return { messageId: "native-message" };
        case "session.tasks.list":
          return { tasks: [] };
        case "session.tasks.cancel":
          return { cancelled: true };
        default:
          return {};
      }
    },
    onNotification: (handler) => {
      notify = handler;
    },
    onRequest: (method, handler) => {
      requestHandlers.set(method, handler);
    },
    onClose: (handler) => {
      closeHandlers.push(handler);
    },
    close: async () => {},
    isClosed: () => false,
    protocolVersion: 4,
  };
  return {
    connection,
    calls,
    named: (method: string) => calls.filter((call) => call.method === method),
    queue: (method: string, result: unknown) => {
      responses.set(method, [...(responses.get(method) ?? []), result]);
    },
    emit: (type: string, data: Record<string, unknown> = {}, extra: Record<string, unknown> = {}) =>
      Effect.sync(() =>
        notify("session.event", { sessionId, event: { type, data, id: type, ...extra } }),
      ),
    serverRequest: (method: string, params: Record<string, unknown>) =>
      requestHandlers.get(method)!({ sessionId, ...params }),
    waitForCalls: Effect.fnUntraced(function* (method: string, count: number) {
      while (calls.filter((call) => call.method === method).length < count)
        yield* Queue.take(requests);
    }),
    takeCall: Effect.fnUntraced(function* (method: string) {
      while (true) {
        const call = yield* Queue.take(requests);
        if (call.method === method) return call;
      }
    }),
    exit: (code = 1) =>
      Effect.sync(() => {
        for (const handler of closeHandlers) handler({ code, signal: null, stderr: "boom" });
      }),
  };
});

const makeHarness = Effect.fnUntraced(function* (
  fake: Effect.Success<ReturnType<typeof makeFakeServer>>,
  policy = fullAccess,
  chosen = selection,
  extra: Partial<CopilotSdkAdapterV2Options> = {},
) {
  const adapter = yield* makeCopilotSdkAdapterV2({
    ...extra,
    instanceId: INSTANCE_ID,
    resolveSpawn: (cwd) => Effect.succeed({ command: "copilot", args: [], env: {}, cwd }),
    openConnection: async () => fake.connection,
    interruptSettleMs: 200,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: ProviderSessionId.make("session-copilot"),
    modelSelection: chosen,
    runtimePolicy: policy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const allEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        allEvents.push(event);
        yield* Queue.offer(emitted, event);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: chosen,
    runtimePolicy: policy,
  });
  const takeEvent = Effect.fnUntraced(function* <
    T extends ProviderAdapter.ProviderAdapterV2Event["type"],
  >(
    type: T,
    predicate: (
      event: Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>,
    ) => boolean = () => true,
  ) {
    while (true) {
      const event = yield* Queue.take(emitted);
      if (
        event.type === type &&
        predicate(event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>)
      )
        return event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>;
    }
  });
  return { runtime, providerThread, takeEvent, allEvents, policy };
});

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  runOrdinal = 1,
  policy = fullAccess,
  text = "Hello Copilot",
): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2TurnInput> {
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: THREAD_ID,
    projectId: ProjectId.make("project-copilot-test"),
    title: "Copilot test",
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
    runId: RunId.make(`run-${runOrdinal}`),
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`attempt-${runOrdinal}`),
    rootNodeId: NodeId.make(`root-${runOrdinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message-${runOrdinal}`),
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection,
    runtimePolicy: policy,
  };
});

const startTurn = Effect.fnUntraced(function* (
  harness: Effect.Success<ReturnType<typeof makeHarness>>,
  fake: Effect.Success<ReturnType<typeof makeFakeServer>>,
  ordinal = 1,
  policy = fullAccess,
) {
  const input = yield* turnInput(harness.providerThread, ordinal, policy);
  yield* harness.runtime.startTurn(input);
  yield* fake.takeCall("session.send");
  const event = yield* harness.takeEvent(
    "provider_turn.updated",
    (candidate) => candidate.providerTurn.status === "running",
  );
  return { input, providerTurn: event.providerTurn };
});

const journalEvent = (
  id: string,
  type: string,
  parentId: string | null,
  extra: {
    readonly content?: string;
    readonly agentId?: string;
    readonly delivery?: string;
    readonly at?: string;
  } = {},
) => ({
  id,
  type,
  parentId,
  timestamp: extra.at ?? "2026-11-14T10:00:00.000Z",
  ...(extra.agentId === undefined ? {} : { agentId: extra.agentId }),
  data: {
    ...(extra.content === undefined ? {} : { content: extra.content }),
    ...(extra.delivery === undefined ? {} : { delivery: extra.delivery }),
  },
});

const turnRecord = (
  ordinal: number,
  ref: { readonly id: string; readonly strength: "strong" | "weak" } | null,
  at = "2026-11-14T10:00:00.000Z",
  until = "2026-11-14T10:00:05.000Z",
): OrchestrationV2ProviderTurn => ({
  id: ProviderTurnId.make(`pt-${ordinal}`),
  providerThreadId: ProviderThreadId.make("pthread"),
  nodeId: NodeId.make(`node-${ordinal}`),
  runAttemptId: RunAttemptId.make(`attempt-${ordinal}`),
  nativeTurnRef:
    ref === null ? null : { driver: COPILOT, nativeId: ref.id, strength: ref.strength },
  ordinal,
  status: "completed",
  startedAt: DateTime.makeUnsafe(at),
  completedAt: DateTime.makeUnsafe(until),
});

describe("Copilot thread operations (journal helpers)", () => {
  it("only counts a root message that is not steering as the start of a turn", () => {
    assert.isTrue(isTurnStart({ type: "user.message", agentId: undefined, delivery: "idle" }));
    assert.isTrue(isTurnStart({ type: "user.message", agentId: undefined, delivery: "queued" }));
    assert.isFalse(isTurnStart({ type: "user.message", agentId: undefined, delivery: "steering" }));
    assert.isFalse(isTurnStart({ type: "user.message", agentId: "agent-1", delivery: "idle" }));
    assert.isFalse(
      isTurnStart({ type: "assistant.message", agentId: undefined, delivery: undefined }),
    );
  });

  it("discards the turns after the target, oldest first, or all of them at thread start", () => {
    const turns = [turnRecord(3, null), turnRecord(1, null), turnRecord(2, null)];
    assert.deepStrictEqual(
      discardedTurns({
        target: { type: "provider_turn", providerTurn: turns[1]! },
        providerThreadTurns: turns,
      }).map((turn) => turn.ordinal),
      [2, 3],
    );
    assert.deepStrictEqual(
      discardedTurns({ target: { type: "thread_start" }, providerThreadTurns: turns }).map(
        (turn) => turn.ordinal,
      ),
      [1, 2, 3],
    );
  });

  it("cuts at the first discarded turn's strong ref when Copilot knows that event", () => {
    const turns = [
      turnRecord(2, { id: "evt-2", strength: "strong" }),
      turnRecord(3, { id: "evt-3", strength: "strong" }),
    ];
    const candidates = [
      { id: "evt-1", timestampMs: 1 },
      { id: "evt-2", timestampMs: 2 },
      { id: "evt-3", timestampMs: 3 },
    ];
    assert.strictEqual(resolveBoundaryEvent(candidates, turns), "evt-2");
    // A ref Copilot does not list (a system wake) is not a place to cut; the next turn decides.
    assert.strictEqual(
      resolveBoundaryEvent(candidates, [
        turnRecord(
          2,
          { id: "wake-9", strength: "strong" },
          "2020-01-01T00:00:00Z",
          "2020-01-01T00:00:01Z",
        ),
        ...turns.slice(1),
      ]),
      "evt-3",
    );
  });

  it("finds a turn T3 could not tie to an event by when it ran, and skips turns with none", () => {
    const at = Date.parse("2026-11-14T10:00:01.000Z");
    const candidates = [{ id: "evt-late", timestampMs: at }];
    assert.strictEqual(
      resolveBoundaryEvent(candidates, [turnRecord(2, { id: "random", strength: "weak" })]),
      "evt-late",
    );
    assert.strictEqual(
      resolveBoundaryEvent(candidates, [
        turnRecord(2, null, "2026-11-14T09:00:00.000Z", "2026-11-14T09:00:05.000Z"),
      ]),
      undefined,
    );
  });

  it("forks before a turn by cutting at the event that preceded its message", () => {
    const events = [
      parseJournalEvent(journalEvent("a", "session.start", null))!,
      parseJournalEvent(journalEvent("b", "user.message", "a", { content: "hi" }))!,
    ];
    assert.strictEqual(forkCutoffBefore(events, "b"), "a");
    assert.strictEqual(forkCutoffBefore(events, "a"), null);
    assert.strictEqual(forkCutoffBefore(events, "missing"), null);
  });

  it.effect("reads a journal across pages and reports one that outruns the reader", () =>
    Effect.gen(function* () {
      const asked: Array<Record<string, unknown>> = [];
      const pages = [
        {
          events: [journalEvent("1", "user.message", null, { content: "a" })],
          cursor: "c1",
          hasMore: true,
          cursorStatus: "ok",
        },
        {
          events: [journalEvent("2", "assistant.message", "1", { content: "b" })],
          cursor: "c2",
          hasMore: false,
          cursorStatus: "ok",
        },
      ];
      const journal = yield* readJournal((_method, params) => {
        asked.push(params);
        return Effect.succeed(pages.shift());
      }, "native-1");
      assert.deepStrictEqual(
        journal.events.map((event) => event.id),
        ["1", "2"],
      );
      assert.isFalse(journal.truncated);
      assert.strictEqual(asked[0]?.cursor, undefined);
      assert.strictEqual(asked[1]?.cursor, "c1");
      const expired = yield* readJournal(
        () => Effect.succeed({ events: [], cursor: "x", hasMore: true, cursorStatus: "expired" }),
        "native-1",
      );
      assert.isTrue(expired.truncated);
    }),
  );

  it("turns the main agent's messages into a snapshot and leaves subagents and blanks out", () => {
    const events = [
      journalEvent("u1", "user.message", null, { content: "question", delivery: "idle" }),
      journalEvent("s1", "user.message", "u1", { content: "sub prompt", agentId: "agent-1" }),
      journalEvent("a1", "assistant.message", "u1", { content: "answer" }),
      journalEvent("a2", "assistant.message", "a1", { content: "   " }),
      journalEvent("t1", "tool.execution_start", "a1"),
    ].map((event) => parseJournalEvent(event)!);
    const messages = journalMessages({
      events,
      threadId: THREAD_ID,
      fallbackTime: DateTime.makeUnsafe("2026-11-14T10:00:00.000Z"),
      messageId: (eventId) => MessageId.make(`m-${eventId}`),
    });
    assert.deepStrictEqual(
      messages.map((message) => [message.id, message.role, message.text]),
      [
        ["m-u1", "user", "question"],
        ["m-a1", "assistant", "answer"],
      ],
    );
  });
});

describe("CopilotSdkAdapterV2 thread operations", () => {
  const finishTurn = Effect.fnUntraced(function* (
    harness: Effect.Success<ReturnType<typeof makeHarness>>,
    fake: Effect.Success<ReturnType<typeof makeFakeServer>>,
    ordinal: number,
    eventId: string,
  ) {
    const started = yield* startTurn(harness, fake, ordinal);
    yield* fake.emit(
      "user.message",
      { content: `turn ${ordinal}`, delivery: "idle" },
      { id: eventId },
    );
    yield* harness.takeEvent(
      "provider_turn.updated",
      (event) => event.providerTurn.nativeTurnRef?.strength === "strong",
    );
    yield* fake.emit("assistant.idle");
    const done = yield* harness.takeEvent(
      "provider_turn.updated",
      (event) =>
        event.providerTurn.id === started.providerTurn.id &&
        event.providerTurn.status === "completed",
    );
    yield* harness.takeEvent("turn.terminal");
    return done.providerTurn;
  });

  it("reports snapshot, rollback and fork as supported", () => {
    assert.isTrue(CopilotSdkCapabilitiesV2.threads.canReadThreadSnapshot);
    assert.isTrue(CopilotSdkCapabilitiesV2.threads.canRollbackThread);
    assert.isTrue(CopilotSdkCapabilitiesV2.threads.canForkThread);
    assert.isTrue(CopilotSdkCapabilitiesV2.threads.canForkFromTurn);
    assert.isFalse(CopilotSdkCapabilitiesV2.threads.canForkFromSubagentThread);
    assert.isTrue(CopilotSdkCapabilitiesV2.checkpointing.providerCanRollbackConversation);
    assert.isTrue(CopilotSdkCapabilitiesV2.checkpointing.providerRollbackReturnsSnapshot);
    assert.isTrue(CopilotSdkCapabilitiesV2.checkpointing.providerCanReadConversationSnapshot);
  });

  it.live(
    "keeps the event that began a turn as its strong ref, and ignores steering and subagents",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeServer();
        const harness = yield* makeHarness(fake);
        yield* startTurn(harness, fake);
        yield* fake.emit(
          "user.message",
          { content: "sub", delivery: "idle" },
          { id: "evt-sub", agentId: "agent-1" },
        );
        yield* fake.emit(
          "user.message",
          { content: "steer", delivery: "steering" },
          { id: "evt-steer" },
        );
        yield* fake.emit(
          "user.message",
          { content: "first", delivery: "idle" },
          { id: "evt-first" },
        );
        const updated = yield* harness.takeEvent(
          "provider_turn.updated",
          (event) => event.providerTurn.nativeTurnRef?.strength === "strong",
        );
        assert.strictEqual(updated.providerTurn.nativeTurnRef?.nativeId, "evt-first");
        // The first message wins; later user messages never move the cut point.
        yield* fake.emit(
          "user.message",
          { content: "again", delivery: "queued" },
          { id: "evt-again" },
        );
        yield* fake.emit("assistant.idle");
        const done = yield* harness.takeEvent(
          "provider_turn.updated",
          (event) => event.providerTurn.status === "completed",
        );
        assert.strictEqual(done.providerTurn.nativeTurnRef?.nativeId, "evt-first");
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("rewinds the conversation only, to the first discarded turn's message", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      const turn2 = yield* finishTurn(harness, fake, 2, "evt-2");
      fake.queue("session.history.listRewindPoints", {
        fileChangeTrackingEnabled: false,
        points: [
          { eventId: "evt-1", timestamp: "2026-11-14T10:00:00Z" },
          { eventId: "evt-2", timestamp: "2026-11-14T10:00:10Z" },
        ],
      });
      fake.queue("session.history.rewind", { outcome: "success", eventsRemoved: 4 });
      const snapshot = yield* harness.runtime.rollbackThread({
        providerThread: harness.providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("cp-1"),
          appRunOrdinal: 1,
          providerTurn: turn1,
        },
        providerThreadTurns: [turn1, turn2],
      });
      const rewind = fake.named("session.history.rewind");
      assert.strictEqual(rewind.length, 1);
      assert.deepStrictEqual(
        { eventId: rewind[0]?.params.eventId, mode: rewind[0]?.params.mode },
        { eventId: "evt-2", mode: "conversation" },
      );
      assert.strictEqual(snapshot.providerThread.id, harness.providerThread.id);
      assert.strictEqual(snapshot.providerThread.nativeConversationHeadRef?.nativeId, "evt-1");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("rolls back to the start of a thread through its first turn's message", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      fake.queue("session.history.listRewindPoints", {
        fileChangeTrackingEnabled: false,
        points: [{ eventId: "evt-1", timestamp: "2026-11-14T10:00:00Z" }],
      });
      fake.queue("session.history.rewind", { outcome: "success" });
      const snapshot = yield* harness.runtime.rollbackThread({
        providerThread: harness.providerThread,
        target: { type: "thread_start", checkpointId: CheckpointId.make("cp-0"), appRunOrdinal: 0 },
        providerThreadTurns: [turn1],
      });
      assert.strictEqual(fake.named("session.history.rewind")[0]?.params.eventId, "evt-1");
      assert.isNull(snapshot.providerThread.nativeConversationHeadRef ?? null);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("leaves Copilot's history alone when no discarded turn has a place to cut", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      const wake = turnRecord(
        2,
        { id: "wake-1", strength: "weak" },
        "2020-01-01T00:00:00Z",
        "2020-01-01T00:00:01Z",
      );
      fake.queue("session.history.listRewindPoints", {
        fileChangeTrackingEnabled: false,
        points: [{ eventId: "evt-1", timestamp: "2026-11-14T10:00:00Z" }],
      });
      yield* harness.runtime.rollbackThread({
        providerThread: harness.providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("cp-1"),
          appRunOrdinal: 1,
          providerTurn: turn1,
        },
        providerThreadTurns: [turn1, wake],
      });
      assert.strictEqual(fake.named("session.history.rewind").length, 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses to roll back a running turn, an unavailable session, or a failed rewind", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      const rollback = harness.runtime.rollbackThread({
        providerThread: harness.providerThread,
        target: { type: "thread_start", checkpointId: CheckpointId.make("cp-0"), appRunOrdinal: 0 },
        providerThreadTurns: [turn1],
      });
      fake.queue("session.history.listRewindPoints", {
        fileChangeTrackingEnabled: true,
        unavailableReason: "session-busy",
        points: [],
      });
      const busy = yield* Effect.flip(rollback);
      assert.strictEqual(busy._tag, "ProviderAdapterRollbackThreadError");
      fake.queue("session.history.listRewindPoints", {
        fileChangeTrackingEnabled: false,
        points: [{ eventId: "evt-1", timestamp: "2026-11-14T10:00:00Z" }],
      });
      fake.queue("session.history.rewind", { outcome: "truncation-failed" });
      const failed = yield* Effect.flip(rollback);
      assert.strictEqual(failed._tag, "ProviderAdapterRollbackThreadError");
      // A turn still running blocks the rollback before Copilot is asked anything.
      yield* startTurn(harness, fake, 2);
      const before = fake.named("session.history.listRewindPoints").length;
      const running = yield* Effect.flip(rollback);
      assert.strictEqual(running._tag, "ProviderAdapterRollbackThreadError");
      assert.strictEqual(fake.named("session.history.listRewindPoints").length, before);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  const journalFor = () => ({
    events: [
      journalEvent("s0", "session.start", null),
      journalEvent("evt-1", "user.message", "s0", { content: "first question", delivery: "idle" }),
      journalEvent("a1", "assistant.message", "evt-1", { content: "first answer" }),
      journalEvent("evt-2", "user.message", "a1", { content: "second question", delivery: "idle" }),
      journalEvent("a2", "assistant.message", "evt-2", { content: "second answer" }),
    ],
    cursor: "end",
    hasMore: false,
    cursorStatus: "ok",
  });

  it.live("forks through a turn by cutting just before the next turn's message", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      const turn2 = yield* finishTurn(harness, fake, 2, "evt-2");
      const sourceNative = harness.providerThread.nativeThreadRef?.nativeId;
      fake.queue("sessions.readPersistedEvents", journalFor());
      fake.queue("sessions.fork", { sessionId: "forked-session" });
      const forked = yield* harness.runtime.forkThread({
        sourceProviderThread: harness.providerThread,
        sourceProviderTurns: [turn1, turn2],
        providerTurnId: turn1.id,
        targetThreadId: ThreadId.make("thread-fork-target"),
      });
      const fork = fake.named("sessions.fork")[0]!;
      assert.deepStrictEqual(
        { sessionId: fork.params.sessionId, toEventId: fork.params.toEventId },
        { sessionId: sourceNative, toEventId: "a1" },
      );
      assert.strictEqual(fake.named("session.resume").at(-1)?.params.sessionId, "forked-session");
      assert.strictEqual(forked.nativeThreadRef?.nativeId, "forked-session");
      assert.strictEqual(forked.appThreadId, ThreadId.make("thread-fork-target"));
      assert.notStrictEqual(forked.id, harness.providerThread.id);
      assert.deepStrictEqual(forked.forkedFrom, {
        providerThreadId: harness.providerThread.id,
        providerTurnId: turn1.id,
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("forks the whole thread, without reading the journal, when no turn is named", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      fake.queue("sessions.fork", { sessionId: "forked-all" });
      const forked = yield* harness.runtime.forkThread({
        sourceProviderThread: harness.providerThread,
        sourceProviderTurns: [turn1],
        targetThreadId: ThreadId.make("thread-fork-all"),
      });
      assert.strictEqual(fake.named("sessions.fork")[0]?.params.toEventId, undefined);
      assert.strictEqual(fake.named("sessions.readPersistedEvents").length, 0);
      assert.strictEqual(forked.nativeThreadRef?.nativeId, "forked-all");
      // Forking from the last turn keeps everything, so it needs no cut either.
      fake.queue("sessions.fork", { sessionId: "forked-last" });
      yield* harness.runtime.forkThread({
        sourceProviderThread: harness.providerThread,
        sourceProviderTurns: [turn1],
        providerTurnId: turn1.id,
        targetThreadId: ThreadId.make("thread-fork-last"),
      });
      assert.strictEqual(fake.named("sessions.fork")[1]?.params.toEventId, undefined);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails a fork whose target turn or session is missing", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const turn1 = yield* finishTurn(harness, fake, 1, "evt-1");
      const missingTurn = yield* Effect.flip(
        harness.runtime.forkThread({
          sourceProviderThread: harness.providerThread,
          sourceProviderTurns: [turn1],
          providerTurnId: ProviderTurnId.make("pt-nope"),
          targetThreadId: ThreadId.make("thread-x"),
        }),
      );
      assert.strictEqual(missingTurn._tag, "ProviderAdapterForkThreadError");
      fake.queue("sessions.fork", {});
      const noSession = yield* Effect.flip(
        harness.runtime.forkThread({
          sourceProviderThread: harness.providerThread,
          sourceProviderTurns: [turn1],
          targetThreadId: ThreadId.make("thread-y"),
        }),
      );
      assert.strictEqual(noSession._tag, "ProviderAdapterForkThreadError");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("reads a thread snapshot from the persisted journal", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      fake.queue("sessions.readPersistedEvents", journalFor());
      const snapshot = yield* harness.runtime.readThreadSnapshot({
        providerThread: harness.providerThread,
      });
      assert.deepStrictEqual(
        snapshot.messages.map((message) => [message.role, message.text]),
        [
          ["user", "first question"],
          ["assistant", "first answer"],
          ["user", "second question"],
          ["assistant", "second answer"],
        ],
      );
      assert.strictEqual(
        fake.named("sessions.readPersistedEvents")[0]?.params.sessionId,
        harness.providerThread.nativeThreadRef?.nativeId,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});

describe("CopilotSdkAdapterV2 subagent tool rows", () => {
  const started = {
    toolCallId: "tool-sub",
    agentName: "general-purpose",
    agentDisplayName: "echo-agent",
    agentDescription: "Run echo",
  };
  const toolStart = {
    toolCallId: "tc-bash",
    toolName: "bash",
    toolTitle: "echo hi",
    arguments: { command: "echo hi" },
    parentToolCallId: "tool-sub",
  };

  it.live(
    "shows a subagent's tool calls in its child thread, even after the parent turn ends",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeServer();
        const harness = yield* makeHarness(fake);
        yield* startTurn(harness, fake);
        yield* fake.emit("subagent.started", started, { agentId: "agent-1" });
        const created = yield* harness.takeEvent("app_thread.created");
        const childThreadId = created.appThread.id;
        yield* fake.emit("assistant.idle");
        yield* harness.takeEvent("turn.terminal");

        yield* fake.emit("tool.execution_start", toolStart, { agentId: "agent-1" });
        // The node is published just before its row.
        const node = yield* harness.takeEvent(
          "node.updated",
          (event) => event.node.kind === "tool_call" && event.node.threadId === childThreadId,
        );
        const running = yield* harness.takeEvent(
          "turn_item.updated",
          (event) => event.turnItem.threadId === childThreadId,
        );
        assert.strictEqual(running.turnItem.status, "running");
        assert.strictEqual(running.turnItem.runId, null);
        assert.strictEqual(running.turnItem.providerTurnId, null);
        assert.strictEqual(node.node.id, running.turnItem.nodeId);
        assert.strictEqual(node.node.parentNodeId, node.node.rootNodeId);

        // Shell output carries only the agent's id, never the parent tool call.
        yield* fake.emit(
          "tool.shell_output",
          { toolCallId: "tc-bash", text: "hi\n", sequence: 1 },
          { agentId: "agent-1" },
        );
        const delta = yield* harness.takeEvent("command_output.delta");
        assert.strictEqual(delta.threadId, childThreadId);
        assert.strictEqual(delta.itemId, running.turnItem.id);
        assert.strictEqual(delta.chunk, "hi\n");

        yield* fake.emit(
          "tool.execution_complete",
          {
            toolCallId: "tc-bash",
            success: true,
            parentToolCallId: "tool-sub",
            result: { content: "hi" },
          },
          { agentId: "agent-1" },
        );
        const done = yield* harness.takeEvent(
          "turn_item.updated",
          (event) =>
            event.turnItem.threadId === childThreadId && event.turnItem.status === "completed",
        );
        assert.strictEqual(done.turnItem.id, running.turnItem.id);
        // The agent's tool rows never appear in the parent thread.
        assert.isFalse(
          harness.allEvents.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.threadId === THREAD_ID &&
              event.turnItem.nativeItemRef?.nativeId === "tc-bash",
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("ends tool rows still running when the agent is stopped or the connection drops", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.emit("subagent.started", started, { agentId: "agent-1" });
      const created = yield* harness.takeEvent("app_thread.created");
      yield* fake.emit("tool.execution_start", toolStart, { agentId: "agent-1" });
      yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.threadId === created.appThread.id && event.turnItem.status === "running",
      );
      yield* fake.emit(
        "subagent.completed",
        { ...started, cancelled: true },
        { agentId: "agent-1" },
      );
      const closed = yield* harness.takeEvent(
        "turn_item.updated",
        (event) =>
          event.turnItem.threadId === created.appThread.id && event.turnItem.status !== "running",
      );
      assert.strictEqual(closed.turnItem.status, "cancelled");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("keeps dedicated rows out of the tool list", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.emit("subagent.started", started, { agentId: "agent-1" });
      const created = yield* harness.takeEvent("app_thread.created");
      yield* fake.emit(
        "tool.execution_start",
        { toolCallId: "tc-intent", toolName: "report_intent", parentToolCallId: "tool-sub" },
        { agentId: "agent-1" },
      );
      yield* fake.emit("tool.execution_start", toolStart, { agentId: "agent-1" });
      const first = yield* harness.takeEvent(
        "turn_item.updated",
        (event) => event.turnItem.threadId === created.appThread.id,
      );
      assert.strictEqual(first.turnItem.nativeItemRef?.nativeId, "tc-bash");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
