import {
  CommandId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";

/**
 * The most queued messages one turn takes in. A batch is taken once, when the
 * turn starts, so a turn never stays open while senders keep writing.
 */
export const QUEUED_MESSAGE_BATCH_LIMIT = 8;

type BatchProjection = Pick<OrchestrationV2ThreadProjection, "runs" | "messages" | "providerTurns">;

/**
 * Queued agent messages that can join `target`'s turn, in delivery order. The
 * batch stops at the first message that has to run as its own turn, so no
 * message is delivered ahead of one queued before it.
 */
export function queuedRunsToBatch(
  projection: BatchProjection,
  target: OrchestrationV2Run,
  limit = QUEUED_MESSAGE_BATCH_LIMIT,
): ReadonlyArray<OrchestrationV2Run> {
  const batch: Array<OrchestrationV2Run> = [];
  for (const run of queuedRunsInDeliveryOrder(projection)) {
    if (batch.length >= limit) break;
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    if (
      message === undefined ||
      // Operator messages keep their own turn.
      message.createdBy !== "agent" ||
      message.delegatedCompletion !== undefined ||
      message.notification !== undefined ||
      message.attachments.length > 0 ||
      // Slash commands (/compact, /goal, ...) change the session; they run alone.
      message.text.trimStart().startsWith("/") ||
      run.freshContext === true ||
      run.restartContinuationOfRunId !== undefined ||
      run.providerInstanceId !== target.providerInstanceId ||
      JSON.stringify(run.modelSelection) !== JSON.stringify(target.modelSelection)
    ) {
      break;
    }
    batch.push(run);
  }
  return batch;
}

/** The thread's running run whose provider turn can take steering input now. */
function steerableRun(projection: BatchProjection): OrchestrationV2Run | undefined {
  return projection.runs
    .filter(
      (run) =>
        run.status === "running" &&
        run.activeAttemptId !== null &&
        projection.providerTurns.some(
          (turn) => turn.runAttemptId === run.activeAttemptId && turn.status === "running",
        ),
    )
    .toSorted((left, right) => right.ordinal - left.ordinal)[0];
}

export class QueuedMessageBatchReactor extends Context.Service<
  QueuedMessageBatchReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly handle: (event: OrchestrationV2DomainEvent) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/QueuedMessageBatchReactor") {}

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  // Each run attempt takes one batch, when its turn starts. The attempt is checked
  // before any read: a running turn reports many updates.
  const handledAttempts = new Set<string>();

  // A queue drained one message per turn falls behind when agents write faster
  // than a turn finishes. When a turn starts, the next queued agent messages join
  // it, so one turn answers several of them.
  const handle: QueuedMessageBatchReactor["Service"]["handle"] = Effect.fnUntraced(
    function* (event) {
      if (event.type !== "provider-turn.updated" || event.payload.status !== "running") return;
      const attemptId = event.payload.runAttemptId;
      if (attemptId === null || handledAttempts.has(attemptId)) return;
      handledAttempts.add(attemptId);
      const records = yield* projections.getThreadRecords(event.threadId, [
        "runs",
        "providerTurns",
      ]);
      if (records.thread.archivedAt !== null || records.thread.deletedAt !== null) return;
      const target = steerableRun({ ...records, messages: [] });
      if (target === undefined || target.activeAttemptId !== attemptId) return;
      const queuedMessageIds = records.runs
        .filter((run) => run.status === "queued")
        .map((run) => run.userMessageId);
      if (queuedMessageIds.length === 0) return;
      // Only the queued messages: a busy thread has thousands of older ones.
      const { messages } = yield* projections.getThreadRecords(event.threadId, ["messages"], {
        messageIds: queuedMessageIds,
      });
      const projection = { ...records, messages };
      for (const queued of queuedRunsToBatch(projection, target)) {
        const promoted = yield* engine
          .dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make(`server:queued-message-batch:${queued.id}`),
            threadId: event.threadId,
            queuedRunId: queued.id,
            targetRunId: target.id,
          })
          .pipe(
            Effect.as(true),
            Effect.catchCause((cause) =>
              Effect.logWarning("batching a queued message into the running turn failed", {
                threadId: event.threadId,
                queuedRunId: queued.id,
                targetRunId: target.id,
                cause,
              }).pipe(Effect.as(false)),
            ),
          );
        // Later messages stay queued behind the one that could not join.
        if (!promoted) break;
      }
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("batching queued messages failed", {
            threadId: event.threadId,
            cause,
          }),
        ),
      ),
  );

  const start: QueuedMessageBatchReactor["Service"]["start"] = () =>
    forkParked(
      Stream.runForEach(engine.streamDomainEvents, handle).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("queued message batch event stream failed", { cause }),
        ),
      ),
    );

  return { start, handle } satisfies QueuedMessageBatchReactor["Service"];
});

export const layer = Layer.effect(QueuedMessageBatchReactor, make);
