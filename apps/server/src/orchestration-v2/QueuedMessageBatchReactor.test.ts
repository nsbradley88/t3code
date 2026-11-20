import { assert, it } from "@effect/vitest";
import type { OrchestrationV2Run, OrchestrationV2ServerCommand } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as QueuedMessageBatchReactor from "./QueuedMessageBatchReactor.ts";

const modelSelection = { instanceId: "claude_work", model: "claude-opus-5-5" };
const activeAttemptId = "attempt:active";

const run = (id: string, ordinal: number, overrides: Partial<Record<string, unknown>> = {}) =>
  ({
    id,
    ordinal,
    status: "queued",
    queuePosition: null,
    activeAttemptId: null,
    providerInstanceId: "claude_work",
    modelSelection,
    userMessageId: `message:${id}`,
    ...overrides,
  }) as unknown as OrchestrationV2Run;

const message = (runId: string, overrides: Partial<Record<string, unknown>> = {}) =>
  ({
    id: `message:${runId}`,
    role: "user",
    text: `Update from ${runId}.`,
    attachments: [],
    createdBy: "agent",
    creationSource: "mcp",
    ...overrides,
  }) as never;

const target = run("run:active", 1, { status: "running", activeAttemptId });

const makeProjection = (
  queued: ReadonlyArray<{
    readonly id: string;
    readonly run?: Partial<Record<string, unknown>>;
    readonly message?: Partial<Record<string, unknown>>;
  }>,
) => ({
  runs: [target, ...queued.map((entry, index) => run(entry.id, index + 2, entry.run))],
  messages: queued.map((entry) => message(entry.id, entry.message)),
  providerTurns: [{ runAttemptId: activeAttemptId, status: "running" }] as never,
});

it.each([
  { case: "agent messages", queued: [{ id: "a" }, { id: "b" }], expected: ["a", "b"] },
  {
    case: "an operator message",
    queued: [{ id: "a" }, { id: "op", message: { createdBy: "user" } }, { id: "c" }],
    expected: ["a"],
  },
  {
    case: "a slash command",
    queued: [{ id: "slash", message: { text: "/compact" } }, { id: "b" }],
    expected: [],
  },
  {
    case: "a fresh session",
    queued: [{ id: "a" }, { id: "fresh", run: { freshContext: true } }],
    expected: ["a"],
  },
  {
    case: "another model",
    queued: [{ id: "other", run: { modelSelection: { ...modelSelection, model: "x" } } }],
    expected: [],
  },
  {
    case: "attachments",
    queued: [{ id: "a" }, { id: "file", message: { attachments: [{ id: "f" }] } }],
    expected: ["a"],
  },
  {
    case: "a completion delivery",
    queued: [{ id: "done", message: { delegatedCompletion: { taskId: "t" } } }],
    expected: [],
  },
])("batches queued messages up to $case", ({ queued, expected }) => {
  assert.deepEqual(
    QueuedMessageBatchReactor.queuedRunsToBatch(makeProjection(queued), target).map(
      (candidate) => candidate.id,
    ),
    expected,
  );
});

it("takes at most the batch limit", () => {
  const queued = Array.from({ length: 12 }, (_, index) => ({ id: `q${index}` }));
  assert.lengthOf(
    QueuedMessageBatchReactor.queuedRunsToBatch(makeProjection(queued), target),
    QueuedMessageBatchReactor.QUEUED_MESSAGE_BATCH_LIMIT,
  );
});

it.effect("folds queued messages into a turn once, when the turn starts", () =>
  Effect.gen(function* () {
    const projection = makeProjection([
      { id: "a" },
      { id: "b" },
      { id: "op", message: { createdBy: "user" } },
      { id: "d" },
    ]);
    const dispatched: Array<OrchestrationV2ServerCommand> = [];
    const layer = QueuedMessageBatchReactor.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.empty,
            dispatch: (command) =>
              Effect.sync(() => {
                dispatched.push(command);
              }).pipe(Effect.as({} as never)),
          }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadRecords: ((_threadId: string, fields: ReadonlyArray<string>) =>
              Effect.succeed(
                (fields.includes("messages")
                  ? { messages: projection.messages }
                  : {
                      thread: { archivedAt: null, deletedAt: null },
                      runs: projection.runs,
                      providerTurns: projection.providerTurns,
                    }) as never,
              )) as never,
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const reactor = yield* QueuedMessageBatchReactor.QueuedMessageBatchReactor;
      const started = {
        type: "provider-turn.updated",
        threadId: "thread:hub",
        payload: { status: "running", runAttemptId: activeAttemptId },
      } as never;
      yield* reactor.handle(started);
      // The same turn reports running again; it takes no second batch.
      yield* reactor.handle(started);
    }).pipe(Effect.provide(layer));
    assert.deepEqual(
      dispatched.map((command) =>
        command.type === "queued-message.promote-to-steer"
          ? `${command.queuedRunId} -> ${command.targetRunId}`
          : command.type,
      ),
      ["a -> run:active", "b -> run:active"],
    );
  }),
);
