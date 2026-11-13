import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ChatAttachmentId,
  EnvironmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
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
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
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
  type CopilotSdkCommandInfo,
} from "./adapter.ts";
import type { CopilotSdkCloseInfo, CopilotSdkConnection } from "./connection.ts";
import { withCopilotSdkTransport } from "./transport.ts";

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

describe("CopilotSdkAdapterV2", () => {
  it("reports live steering and subagent control", () => {
    assert.strictEqual(CopilotSdkCapabilitiesV2.turns.supportsActiveSteering, true);
    assert.strictEqual(CopilotSdkCapabilitiesV2.turns.activeSteeringInterruptsTools, true);
    assert.strictEqual(CopilotSdkCapabilitiesV2.sessions.supportsRuntimeModeSwitchInSession, true);
  });

  it.live("creates a streaming session with the thread's MCP credential and allow-all", () =>
    Effect.gen(function* () {
      yield* (yield* McpProviderSessions.McpProviderSessions).set({
        environmentId: EnvironmentId.make("test-environment"),
        threadId: THREAD_ID,
        providerSessionId: "test-session",
        providerInstanceId: INSTANCE_ID,
        endpoint: "http://127.0.0.1:43210/mcp",
        authorizationHeader: "Bearer thread-token",
        browserToolsAvailable: true,
      });
      const fake = yield* makeFakeServer();
      yield* makeHarness(fake);
      const create = fake.named("session.create")[0]!;
      assert.strictEqual(create.params.model, MODEL);
      assert.strictEqual(create.params.streaming, true);
      assert.strictEqual(create.params.requestPermission, true);
      assert.deepStrictEqual(create.params.mcpServers, {
        "t3-code": {
          type: "http",
          url: "http://127.0.0.1:43210/mcp",
          headers: { Authorization: "Bearer thread-token" },
          tools: ["*"],
        },
      });
      assert.deepStrictEqual(
        fake.named("session.permissions.setMode")[0]?.params.mode,
        "allow-all",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("lets Copilot choose its own model for the default placeholder", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      yield* makeHarness(fake, fullAccess, { ...selection, model: "default" });
      assert.strictEqual("model" in fake.named("session.create")[0]!.params, false);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("steers a running turn with an immediate send instead of queueing", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const { input, providerTurn } = yield* startTurn(harness, fake);
      assert.strictEqual(fake.named("session.send")[0]!.params.mode, "immediate");
      yield* fake.emit("assistant.turn_start");
      yield* fake.emit("tool.execution_start", {
        toolCallId: "tool-1",
        toolName: "bash",
        arguments: { command: "sleep 60" },
      });
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread: harness.providerThread,
        providerTurnId: providerTurn.id,
        message: { ...input.message, text: "Stop and tell me the time instead" },
      });
      const steer = fake.named("session.send")[1]!;
      assert.strictEqual(steer.params.mode, "immediate");
      assert.strictEqual(steer.params.prompt, "Stop and tell me the time instead");
      // Steering never interrupts or cancels background agents.
      assert.strictEqual(fake.named("session.interruptMainTurn").length, 0);
      assert.strictEqual(fake.named("session.cancelAllBackgroundAgents").length, 0);
      assert.strictEqual(fake.named("session.abort").length, 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("steers while an earlier steer's turn is still running, with no backlog", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const { input, providerTurn } = yield* startTurn(harness, fake);
      for (const text of ["one", "two", "three"])
        yield* harness.runtime.steerTurn({
          threadId: THREAD_ID,
          runId: input.runId,
          providerThread: harness.providerThread,
          providerTurnId: providerTurn.id,
          message: { ...input.message, text },
        });
      const modes = fake.named("session.send").map((call) => call.params.mode);
      assert.deepStrictEqual(modes, ["immediate", "immediate", "immediate", "immediate"]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("sums per-call usage into per-turn usage and completes at assistant.idle", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.emit("assistant.message_delta", { messageId: "m1", deltaContent: "Hel" });
      yield* fake.emit("assistant.message_delta", { messageId: "m1", deltaContent: "lo" });
      yield* fake.emit("assistant.message", { messageId: "m1", content: "Hello" });
      yield* fake.emit("assistant.usage", {
        inputTokens: 100,
        outputTokens: 10,
        cacheReadTokens: 40,
      });
      yield* fake.emit("assistant.usage", {
        inputTokens: 130,
        outputTokens: 20,
        reasoningTokens: 5,
      });
      yield* fake.emit("assistant.idle");
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");
      const done = harness.allEvents.findLast(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      assert.ok(done?.type === "provider_turn.updated");
      assert.deepStrictEqual(
        {
          input: done.providerTurn.turnTokenUsage?.inputTokens,
          output: done.providerTurn.turnTokenUsage?.outputTokens,
        },
        { input: 230, output: 30 },
      );
      const message = harness.allEvents.findLast(
        (event) => event.type === "message.updated" && event.message.role === "assistant",
      );
      assert.ok(message?.type === "message.updated");
      assert.strictEqual(message.message.text, "Hello");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a second turn reports only its own usage", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake, 1);
      yield* fake.emit("assistant.usage", { inputTokens: 500, outputTokens: 50 });
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* startTurn(harness, fake, 2);
      yield* fake.emit("assistant.usage", { inputTokens: 700, outputTokens: 30 });
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      const completed = harness.allEvents.filter(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      const usage = completed.map((event) =>
        event.type === "provider_turn.updated" ? event.providerTurn.turnTokenUsage?.inputTokens : 0,
      );
      assert.deepStrictEqual(usage, [500, 700]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("interrupts the main turn only and leaves background agents running", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const { providerTurn } = yield* startTurn(harness, fake);
      yield* fake.emit("subagent.started", {
        toolCallId: "agent-call-1",
        agentName: "explore",
        agentDisplayName: "Explore",
        agentDescription: "Look around",
      });
      const stopping = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId: providerTurn.id })
        .pipe(Effect.forkScoped);
      const interrupt = yield* fake.takeCall("session.interruptMainTurn");
      assert.strictEqual(interrupt.params.flushQueued, false);
      yield* fake.emit("abort", { reason: "user_initiated" });
      yield* fake.emit("assistant.idle", { aborted: true });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "interrupted");
      yield* Fiber.join(stopping);
      assert.strictEqual(fake.named("session.cancelAllBackgroundAgents").length, 0);
      // The subagent row is not cancelled just because the main turn stopped.
      const subagent = harness.allEvents.findLast((event) => event.type === "subagent.updated");
      assert.ok(subagent?.type === "subagent.updated");
      assert.strictEqual(subagent.subagent.status, "running");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("ends an interrupted turn itself when Copilot stays silent", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const { providerTurn } = yield* startTurn(harness, fake);
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: providerTurn.id,
      });
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("stops one subagent through its task id, resolving the tool call id the row used", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      fake.queue("session.tasks.list", {
        tasks: [
          {
            type: "agent",
            id: "task-77",
            toolCallId: "agent-call-1",
            status: "running",
            displayName: "Explore",
          },
        ],
      });
      yield* fake.emit("subagent.started", {
        toolCallId: "agent-call-1",
        agentName: "explore",
        agentDisplayName: "Explore",
        agentDescription: "Look around",
      });
      yield* harness.takeEvent(
        "provider_thread.updated",
        (event) => (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 1,
      );
      yield* harness.runtime.stopSubagent!({
        providerThread: harness.providerThread,
        nativeTaskId: "agent-call-1",
      });
      assert.deepStrictEqual(fake.named("session.tasks.cancel")[0]!.params.id, "task-77");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails loudly when Copilot cannot cancel the task", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      fake.queue("session.tasks.cancel", { cancelled: false });
      const exit = yield* harness.runtime.stopTask!({
        providerThread: harness.providerThread,
        taskId: "gone",
      }).pipe(Effect.exit);
      assert.strictEqual(exit._tag, "Failure");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("keeps allow-all across plan mode and when Copilot changes its own mode", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const plan = { ...fullAccess, interactionMode: "plan" as const };
      yield* startTurn(harness, fake, 1, plan);
      assert.deepStrictEqual(
        fake.named("session.mode.set").map((call) => call.params.mode),
        ["plan"],
      );
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      // Copilot drops back to interactive after plan mode; permission mode is set again.
      const before = fake.named("session.permissions.setMode").length;
      yield* fake.emit("session.mode_changed", { previousMode: "plan", newMode: "interactive" });
      yield* fake.waitForCalls("session.permissions.setMode", before + 1);
      const after = fake.named("session.permissions.setMode");
      assert.ok(after.length > before);
      assert.strictEqual(after.at(-1)!.params.mode, "allow-all");
      yield* startTurn(harness, fake, 2);
      // Copilot reported interactive itself, so the next turn has no mode to restore.
      assert.deepStrictEqual(
        fake.named("session.mode.set").map((call) => call.params.mode),
        ["plan"],
      );
      assert.ok(
        fake.named("session.permissions.setMode").every((call) => call.params.mode === "allow-all"),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("returns to interactive mode after a plan turn, still on allow-all", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake, 1, { ...fullAccess, interactionMode: "plan" });
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* startTurn(harness, fake, 2);
      assert.deepStrictEqual(
        fake.named("session.mode.set").map((call) => call.params.mode),
        ["plan", "interactive"],
      );
      assert.ok(
        fake.named("session.permissions.setMode").every((call) => call.params.mode === "allow-all"),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("completes a subagent that finishes after the main turn ended", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.emit("subagent.started", {
        toolCallId: "agent-call-9",
        agentName: "explore",
        agentDisplayName: "Explore",
        agentDescription: "Look around",
      });
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* fake.emit("subagent.completed", { toolCallId: "agent-call-9", agentName: "explore" });
      const done = yield* harness.takeEvent(
        "subagent.updated",
        (event) => event.subagent.status === "completed",
      );
      assert.strictEqual(done.subagent.nativeTaskRef?.nativeId, "agent-call-9");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("switches permission mode between turns of one session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake, 1);
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* startTurn(harness, fake, 2, { ...fullAccess, runtimeMode: "approval-required" });
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* startTurn(harness, fake, 3);
      assert.deepStrictEqual(
        fake.named("session.permissions.setMode").map((call) => call.params.mode),
        ["allow-all", "manual", "allow-all"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("round-trips a permission request", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake, { ...fullAccess, runtimeMode: "approval-required" });
      yield* startTurn(harness, fake, 1, { ...fullAccess, runtimeMode: "approval-required" });
      yield* fake.emit("permission.requested", {
        requestId: "perm-1",
        permissionRequest: {
          kind: "shell",
          fullCommandText: "touch result.txt",
          canOfferSessionApproval: true,
          commands: [{ identifier: "touch" }],
        },
      });
      const requested = yield* harness.takeEvent(
        "runtime_request.updated",
        (event) => event.runtimeRequest.status === "pending",
      );
      assert.strictEqual(requested.runtimeRequest.kind, "command");
      yield* harness.runtime.respondToRuntimeRequest({
        requestId: requested.runtimeRequest.id,
        decision: "acceptForSession",
      });
      const answer = fake.named("session.permissions.handlePendingPermissionRequest")[0]!;
      assert.deepStrictEqual(answer.params.result, {
        kind: "approve-for-session",
        approval: { kind: "commands", commandIdentifiers: ["touch"] },
      });
      const resolved = yield* harness.takeEvent(
        "runtime_request.updated",
        (event) => event.runtimeRequest.status === "resolved",
      );
      assert.strictEqual(resolved.runtimeRequest.decision, "acceptForSession");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("answers writes itself in auto-accept-edits mode", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const policy = { ...fullAccess, runtimeMode: "auto-accept-edits" as const };
      const harness = yield* makeHarness(fake, policy);
      yield* startTurn(harness, fake, 1, policy);
      yield* fake.emit("permission.requested", {
        requestId: "perm-w",
        permissionRequest: { kind: "write", fileName: "a.txt", canOfferSessionApproval: true },
      });
      yield* fake.waitForCalls("session.permissions.handlePendingPermissionRequest", 1);
      assert.deepStrictEqual(
        fake.named("session.permissions.handlePendingPermissionRequest")[0]?.params.result,
        { kind: "approve-once" },
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("round-trips a user question through the server request", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      const answer = fake.serverRequest("userInput.request", {
        requestId: "q-1",
        question: "Which database?",
        choices: ["postgres", "sqlite"],
        allowFreeform: true,
      });
      const requested = yield* harness.takeEvent(
        "runtime_request.updated",
        (event) => event.runtimeRequest.status === "pending",
      );
      assert.strictEqual(requested.runtimeRequest.kind, "user_input");
      yield* harness.runtime.respondToRuntimeRequest({
        requestId: requested.runtimeRequest.id,
        answers: { answer: "sqlite" },
      });
      assert.deepStrictEqual(yield* Effect.promise(() => answer), {
        answer: "sqlite",
        wasFreeform: false,
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("publishes a plan card and leaves plan mode without implementing", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake, 1, { ...fullAccess, interactionMode: "plan" });
      const response = fake.serverRequest("exitPlanMode.request", {
        summary: "Plan",
        planContent: "# Plan\n1. Do it",
        actions: ["exit_only", "interactive"],
        recommendedAction: "interactive",
        requestId: "plan-1",
      });
      const plan = yield* harness.takeEvent("plan.updated");
      assert.strictEqual(plan.plan.kind, "proposed_plan");
      assert.strictEqual(
        plan.plan.kind === "proposed_plan" ? plan.plan.markdown : "",
        "# Plan\n1. Do it",
      );
      assert.deepStrictEqual(yield* Effect.promise(() => response), {
        approved: true,
        selectedAction: "exit_only",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("compacts through session.history.compact", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      fake.queue("session.history.compact", {
        success: true,
        tokensRemoved: 1234,
        messagesRemoved: 9,
      });
      yield* harness.runtime.compactThread!(yield* turnInput(harness.providerThread, 1));
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");
      assert.strictEqual(fake.named("session.send").length, 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("fails the turn and ends the stream when the server exits", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.exit(1);
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "failed");
      assert.strictEqual(terminal.threadDisposition, "broken");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("starts a continuation when background work wakes the agent without a turn", () =>
    Effect.gen(function* () {
      const offered: Array<unknown> = [];
      const wakeOffered = yield* Deferred.make<void>();
      const fake = yield* makeFakeServer();
      const adapter = yield* makeCopilotSdkAdapterV2({
        instanceId: INSTANCE_ID,
        resolveSpawn: (cwd) => Effect.succeed({ command: "copilot", args: [], env: {}, cwd }),
        openConnection: async () => fake.connection,
        continuationRequests: {
          offer: (request) =>
            Effect.sync(() => void offered.push(request)).pipe(
              Effect.andThen(Deferred.succeed(wakeOffered, undefined)),
            ),
        },
      });
      const runtime = yield* adapter.openSession({
        threadId: THREAD_ID,
        providerSessionId: ProviderSessionId.make("session-copilot"),
        modelSelection: selection,
        runtimePolicy: fullAccess,
      });
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: selection,
        runtimePolicy: fullAccess,
      });
      yield* fake.emit("assistant.turn_start");
      yield* Deferred.await(wakeOffered);
      assert.strictEqual(offered.length, 1);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});

describe("withCopilotSdkTransport", () => {
  const stub = (label: string, fail = false) =>
    ProviderAdapter.ProviderAdapterV2.of({
      instanceId: INSTANCE_ID,
      driver: COPILOT,
      getCapabilities: () => Effect.succeed(CopilotSdkCapabilitiesV2),
      planSelectionTransition: () => Effect.die("unused"),
      openSession: () =>
        fail
          ? Effect.fail(
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: COPILOT,
                providerSessionId: ProviderSessionId.make("s"),
                cause: new Error("no --headless"),
              }),
            )
          : Effect.succeed({ label } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime),
    });

  it.live("opens on the SDK server when it starts", () =>
    Effect.gen(function* () {
      const adapter = withCopilotSdkTransport(stub("sdk"), stub("acp"));
      const runtime = yield* adapter.openSession({} as never);
      assert.deepStrictEqual(runtime as unknown, { label: "sdk" });
    }),
  );

  it.live("falls back to ACP when the SDK server cannot start", () =>
    Effect.gen(function* () {
      const adapter = withCopilotSdkTransport(stub("sdk", true), stub("acp"));
      const runtime = yield* adapter.openSession({} as never);
      assert.deepStrictEqual(runtime as unknown, { label: "acp" });
    }),
  );
});

describe("CopilotSdkAdapterV2 UI integration", () => {
  const approvalRequired = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "approval-required",
    interactionMode: "default",
    cwd: null,
  });
  const MODE = "https://agentclientprotocol.com/protocol/session-modes#";
  const withOptions = (options: ReadonlyArray<{ id: string; value: string }>) => ({
    ...selection,
    options,
  });

  it.live("applies the picker's Autopilot mode, bypass and reasoning effort", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake, approvalRequired);
      assert.deepStrictEqual(
        fake.named("session.permissions.setMode").map((call) => call.params.mode),
        ["manual"],
      );
      const picked = withOptions([
        { id: "mode", value: `${MODE}autopilot` },
        { id: "reasoning_effort", value: "high" },
      ]);
      const input = {
        ...(yield* turnInput(harness.providerThread, 1, approvalRequired)),
        modelSelection: picked,
      };
      yield* harness.runtime.startTurn(input);
      const send = yield* fake.takeCall("session.send");
      // Autopilot is Copilot's own bypass, so permissions follow the picker, not the lower floor.
      assert.deepStrictEqual(
        fake.named("session.permissions.setMode").map((call) => call.params.mode),
        ["manual", "allow-all"],
      );
      assert.strictEqual(fake.named("session.mode.set")[0]?.params.mode, "autopilot");
      assert.strictEqual(send.params.agentMode, "autopilot");
      assert.strictEqual(
        fake.named("session.model.setReasoningEffort")[0]?.params.reasoningEffort,
        "high",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("turns bypass on from the allow_all option and never lowers full access", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake, approvalRequired);
      const input = {
        ...(yield* turnInput(harness.providerThread, 1, approvalRequired)),
        modelSelection: withOptions([{ id: "allow_all", value: "on" }]),
      };
      yield* harness.runtime.startTurn(input);
      yield* fake.takeCall("session.send");
      assert.strictEqual(
        fake.named("session.permissions.setMode").at(-1)?.params.mode,
        "allow-all",
      );
      yield* Effect.void;

      const fullFake = yield* makeFakeServer();
      const fullHarness = yield* makeHarness(fullFake);
      const offInput = {
        ...(yield* turnInput(fullHarness.providerThread)),
        modelSelection: withOptions([{ id: "allow_all", value: "off" }]),
      };
      yield* fullHarness.runtime.startTurn(offInput);
      yield* fullFake.takeCall("session.send");
      assert.deepStrictEqual(
        fullFake.named("session.permissions.setMode").map((call) => call.params.mode),
        ["allow-all"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("keeps T3 plan mode over the picker and returns to the picked mode afterwards", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const planPolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "plan",
        cwd: null,
      });
      yield* harness.runtime.startTurn({
        ...(yield* turnInput(harness.providerThread, 1, planPolicy)),
        modelSelection: withOptions([{ id: "mode", value: `${MODE}agent` }]),
      });
      yield* fake.takeCall("session.send");
      assert.strictEqual(fake.named("session.mode.set").at(-1)?.params.mode, "plan");
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* harness.runtime.startTurn({
        ...(yield* turnInput(harness.providerThread, 2)),
        modelSelection: withOptions([{ id: "mode", value: `${MODE}agent` }]),
      });
      yield* fake.takeCall("session.send");
      assert.strictEqual(fake.named("session.mode.set").at(-1)?.params.mode, "interactive");
      // Bypass held across the whole plan round trip.
      assert.deepStrictEqual(
        [...new Set(fake.named("session.permissions.setMode").map((call) => call.params.mode))],
        ["allow-all"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("publishes the session's slash commands and refreshes them when they change", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const published = yield* Queue.unbounded<ReadonlyArray<CopilotSdkCommandInfo>>();
      fake.queue("session.commands.list", {
        commands: [
          { name: "review", description: "Review code", input: { hint: "what to review" } },
          { name: "usage", description: "Show usage", aliases: ["u"] },
        ],
      });
      yield* makeHarness(fake, fullAccess, selection, {
        publishCommands: (commands) => Queue.offer(published, commands).pipe(Effect.asVoid),
      });
      assert.deepStrictEqual(yield* Queue.take(published), [
        { name: "review", description: "Review code", hint: "what to review" },
        { name: "usage", description: "Show usage" },
      ]);
      yield* fake.emit("commands.changed", { commands: [{ name: "init", description: "Init" }] });
      assert.deepStrictEqual(yield* Queue.take(published), [{ name: "init", description: "Init" }]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  const commandFake = Effect.fnUntraced(function* () {
    const fake = yield* makeFakeServer();
    fake.queue("session.commands.list", {
      commands: [
        { name: "review", description: "Review code", aliases: ["rv"] },
        { name: "usage", description: "Show usage" },
      ],
    });
    return fake;
  });
  const startCommandTurn = Effect.fnUntraced(function* (
    harness: Effect.Success<ReturnType<typeof makeHarness>>,
    text: string,
  ) {
    yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, 1, fullAccess, text));
  });

  it.live("runs a text-only slash command without a model turn", () =>
    Effect.gen(function* () {
      const fake = yield* commandFake();
      const harness = yield* makeHarness(fake);
      fake.queue("session.commands.invoke", { kind: "text", text: "12 of 300 requests used" });
      yield* startCommandTurn(harness, "/usage");
      const message = yield* harness.takeEvent("message.updated", (event) =>
        event.message.text.includes("12 of 300"),
      );
      assert.strictEqual(message.message.role, "assistant");
      const terminal = yield* harness.takeEvent("turn.terminal");
      assert.strictEqual(terminal.status, "completed");
      assert.deepStrictEqual(fake.named("session.commands.invoke")[0]?.params, {
        sessionId: fake.named("session.create")[0]!.params.sessionId,
        name: "usage",
        input: "",
      });
      assert.strictEqual(fake.named("session.send").length, 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("sends the prompt a slash command expands to, through an alias, and finalizes it", () =>
    Effect.gen(function* () {
      const fake = yield* commandFake();
      const harness = yield* makeHarness(fake);
      fake.queue("session.commands.invoke", {
        kind: "agent-prompt",
        prompt: "Review these files: src",
        displayPrompt: "/review src",
        mode: "plan",
        reservationToken: "token-1",
      });
      yield* startCommandTurn(harness, "/rv src");
      const send = yield* fake.takeCall("session.send");
      assert.strictEqual(fake.named("session.commands.invoke")[0]?.params.name, "rv");
      assert.strictEqual(fake.named("session.commands.invoke")[0]?.params.input, "src");
      assert.ok(String(send.params.prompt).endsWith("Review these files: src"));
      assert.strictEqual(send.params.displayPrompt, "/review src");
      assert.strictEqual(send.params.agentMode, "plan");
      assert.strictEqual(
        fake.named("session.commands.finalizeInvocationEffect")[0]?.params.outcome,
        "applied",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("sends file paths and unknown slash words as ordinary prompts", () =>
    Effect.gen(function* () {
      const fake = yield* commandFake();
      const harness = yield* makeHarness(fake);
      yield* startCommandTurn(harness, "/usr/bin/ls is missing, what now?");
      yield* fake.takeCall("session.send");
      assert.strictEqual(fake.named("session.commands.invoke").length, 0);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("explains commands that need Copilot's interactive terminal", () =>
    Effect.gen(function* () {
      const fake = yield* commandFake();
      const harness = yield* makeHarness(fake);
      fake.queue("session.commands.invoke", { kind: "select-subcommand", command: "usage" });
      yield* startCommandTurn(harness, "/usage");
      const message = yield* harness.takeEvent("message.updated");
      assert.ok(message.message.text.includes("interactive terminal"));
      assert.strictEqual((yield* harness.takeEvent("turn.terminal")).status, "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("publishes Copilot's todo table as a live todo list", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      fake.queue("session.plan.readSqlTodos", {
        rows: [
          { id: "write", title: "Write tests", status: "in_progress" },
          { id: "ship", title: "Ship it", status: "pending" },
          { id: "blank", title: "  ", status: "pending" },
        ],
      });
      yield* fake.emit("session.todos_changed");
      const first = yield* harness.takeEvent("plan.updated");
      assert.strictEqual(first.plan.kind, "todo_list");
      if (first.plan.kind !== "todo_list") return;
      assert.deepStrictEqual(
        first.plan.steps.map((step) => [step.id, step.status]),
        [
          ["write", "running"],
          ["ship", "pending"],
        ],
      );
      assert.strictEqual(first.plan.status, "active");
      fake.queue("session.plan.readSqlTodos", {
        rows: [
          { id: "write", title: "Write tests", status: "done" },
          { id: "ship", title: "Ship it", status: "done" },
        ],
      });
      yield* fake.emit("session.todos_changed");
      const second = yield* harness.takeEvent("plan.updated");
      // The same plan updates in place; it is not a new list per change.
      assert.strictEqual(second.plan.id, first.plan.id);
      assert.strictEqual(second.plan.status, "completed");
      const item = yield* harness.takeEvent(
        "turn_item.updated",
        (event) => event.turnItem.type === "todo_list",
      );
      assert.strictEqual(item.turnItem.type, "todo_list");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("attaches images natively on a turn and on a steer", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      const image = {
        type: "image" as const,
        id: ChatAttachmentId.make("shot-1"),
        name: "shot.png",
        mimeType: "image/png",
        sizeBytes: 12,
      };
      const base = yield* turnInput(harness.providerThread);
      const input = { ...base, message: { ...base.message, attachments: [image] } };
      yield* harness.runtime.startTurn(input);
      const send = yield* fake.takeCall("session.send");
      const attachments = send.params.attachments as Array<Record<string, unknown>>;
      assert.strictEqual(attachments.length, 1);
      assert.strictEqual(attachments[0]?.type, "file");
      assert.strictEqual(attachments[0]?.displayName, "shot.png");
      assert.strictEqual(attachments[0]?.mimeType, "image/png");
      assert.ok(String(attachments[0]?.path).endsWith("shot-1"));
      const running = yield* harness.takeEvent(
        "provider_turn.updated",
        (event) => event.providerTurn.status === "running",
      );
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread: harness.providerThread,
        providerTurnId: running.providerTurn.id,
        message: { ...input.message, text: "look at this instead" },
      });
      const steer = fake.named("session.send")[1]!;
      assert.strictEqual((steer.params.attachments as unknown[]).length, 1);
      assert.strictEqual(steer.params.mode, "immediate");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});

describe("CopilotSdkAdapterV2 subagent threads", () => {
  const started = {
    toolCallId: "tool-sub",
    agentName: "general-purpose",
    agentDisplayName: "echo-agent",
    agentDescription: "Run echo",
  };

  it.live("shows a background agent as a child thread that outlives the parent turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.emit("subagent.started", started, { agentId: "agent-1" });
      const created = yield* harness.takeEvent("app_thread.created");
      assert.strictEqual(created.appThread.lineage.relationshipToParent, "subagent");
      assert.strictEqual(created.appThread.lineage.parentThreadId, THREAD_ID);
      assert.strictEqual(created.appThread.forkedFrom?.type, "node");
      assert.strictEqual(created.appThread.title, "echo-agent");
      const row = yield* harness.takeEvent("subagent.updated");
      assert.strictEqual(row.subagent.childThreadId, created.appThread.id);
      assert.strictEqual(row.subagent.status, "running");
      const root = yield* harness.takeEvent(
        "node.updated",
        (event) => event.node.kind === "root_turn",
      );
      assert.strictEqual(root.node.threadId, created.appThread.id);
      assert.strictEqual(root.node.status, "running");

      // The main loop ends while the agent keeps working; its events still reach the child thread.
      yield* fake.emit("assistant.idle");
      yield* harness.takeEvent("turn.terminal");
      yield* fake.emit(
        "user.message",
        { content: "Run `echo hi`, then reply DONE" },
        { agentId: "agent-1" },
      );
      const prompt = yield* harness.takeEvent(
        "message.updated",
        (event) => event.message.threadId === created.appThread.id,
      );
      assert.strictEqual(prompt.message.role, "user");
      assert.strictEqual(prompt.message.text, "Run `echo hi`, then reply DONE");
      const reprompted = yield* harness.takeEvent(
        "subagent.updated",
        (event) => event.subagent.prompt === "Run `echo hi`, then reply DONE",
      );
      assert.strictEqual(reprompted.subagent.childThreadId, created.appThread.id);

      yield* fake.emit(
        "assistant.message",
        { messageId: "reply-1", content: "DONE", parentToolCallId: "tool-sub" },
        { agentId: "agent-1" },
      );
      const reply = yield* harness.takeEvent(
        "message.updated",
        (event) =>
          event.message.role === "assistant" && event.message.threadId === created.appThread.id,
      );
      assert.strictEqual(reply.message.text, "DONE");

      yield* fake.emit("subagent.completed", started, { agentId: "agent-1" });
      const done = yield* harness.takeEvent(
        "subagent.updated",
        (event) => event.subagent.status === "completed",
      );
      assert.strictEqual(done.subagent.result, "DONE");
      const closed = yield* harness.takeEvent(
        "node.updated",
        (event) => event.node.kind === "root_turn" && event.node.status === "completed",
      );
      assert.strictEqual(closed.node.threadId, created.appThread.id);
      // The agent's replies never leak into the parent thread.
      assert.strictEqual(
        harness.allEvents.some(
          (event) =>
            event.type === "message.updated" &&
            event.message.threadId === THREAD_ID &&
            event.message.text === "DONE",
        ),
        false,
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("ends an open child thread as failed when the connection drops", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeServer();
      const harness = yield* makeHarness(fake);
      yield* startTurn(harness, fake);
      yield* fake.emit("subagent.started", started, { agentId: "agent-1" });
      yield* harness.takeEvent("node.updated", (event) => event.node.kind === "root_turn");
      yield* fake.exit();
      const failed = yield* harness.takeEvent(
        "node.updated",
        (event) => event.node.kind === "root_turn" && event.node.status === "failed",
      );
      assert.strictEqual(failed.node.runId, null);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});

// Silence unused-import analysis for the driver constant used by stubs.
void COPILOT;
