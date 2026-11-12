import {
  ProviderDriverKind,
  type ModelSelection,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type OrchestrationV2TurnItemStatus,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type RuntimeMode,
  type RuntimeRequestId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import { buildRuntimeInstructions } from "@t3tools/provider-core/server/runtimeInstructions";

import {
  openCopilotSdkConnection,
  type CopilotSdkConnection,
  type CopilotSdkSpawnInput,
} from "./connection.ts";
import {
  copilotApprovalOptions,
  copilotPermissionDecision,
  copilotPermissionKind,
  copilotPermissionPrompt,
  copilotPermissionModeForRuntimeMode,
  copilotToolPresentation,
  type CopilotPermissionRequest,
  type CopilotPermissionMode,
  type CopilotSessionMode,
} from "./protocol.ts";

const COPILOT_PROVIDER = ProviderDriverKind.make("copilot");

export class CopilotSdkSpawnError extends Schema.TaggedError<CopilotSdkSpawnError>()(
  "CopilotSdkSpawnError",
  { detail: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
const isProtocolError = (cause: unknown): cause is ProviderAdapter.ProviderAdapterProtocolError =>
  typeof cause === "object" &&
  cause !== null &&
  "_tag" in cause &&
  cause._tag === "ProviderAdapterProtocolError";

/**
 * Copilot driven over its own SDK JSON-RPC server, the way T3 Code drives Claude Code and Codex.
 * `session.send` with `mode: "immediate"` reaches a running turn within a second, which is what
 * lets operator input and cross-thread messages steer instead of queueing behind the turn.
 */
export const CopilotSdkCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    // A steer cancels the running tool call and the model answers the new message next.
    activeSteeringInterruptsTools: true,
    activeSteeringAcceptsAttachments: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: true,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface CopilotSdkAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly resolveSpawn: (
    cwd: string,
  ) => Effect.Effect<Omit<CopilotSdkSpawnInput, "platform">, CopilotSdkSpawnError>;
  /** Replaced in tests; defaults to launching `copilot --headless --stdio`. */
  readonly openConnection?: (input: CopilotSdkSpawnInput) => Promise<CopilotSdkConnection>;
  readonly requestTimeoutMs?: number;
  /** How long an interrupt waits for Copilot to settle the turn before the adapter ends it. */
  readonly interruptSettleMs?: number;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

interface SdkEvent {
  readonly type: string;
  readonly agentId?: string;
  readonly data?: Record<string, unknown>;
  readonly timestamp?: string;
}

type Inbox =
  | { readonly type: "event"; readonly event: SdkEvent }
  | { readonly type: "flush"; readonly turn: ActiveTurn }
  | { readonly type: "closed"; readonly detail: string }
  | {
      readonly type: "userInput";
      readonly params: Record<string, unknown>;
      readonly resolve: (value: unknown) => void;
    }
  | {
      readonly type: "exitPlan";
      readonly params: Record<string, unknown>;
      readonly resolve: (value: unknown) => void;
    };

interface TurnItemState {
  readonly kind: "assistant" | "reasoning" | "tool" | "subagent";
  text: string;
  status: OrchestrationV2TurnItemStatus;
  toolName?: string;
  toolTitle?: string;
  args?: Record<string, unknown>;
  exitCode?: number;
  agentName?: string | undefined;
  prompt?: string;
}

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  readonly nativeId: string;
  readonly items: Map<string, TurnItemState>;
  readonly ordinals: Map<string, number>;
  readonly started: Map<string, DateTime.Utc>;
  readonly dirty: Set<string>;
  readonly done: Deferred.Deferred<void>;
  readonly usage: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens: number;
    cacheWriteTokens: number;
    reasoningTokens: number;
  };
  nextOrdinal: number;
  flushScheduled: boolean;
  interruptRequested: boolean;
  aborted: boolean;
  failure: string | undefined;
  compact: boolean;
  hasSubagents: boolean;
  steers: number;
}

interface PendingRequest {
  readonly native:
    | {
        readonly type: "permission";
        readonly request: CopilotPermissionRequest;
        readonly id: string;
      }
    | {
        readonly type: "question";
        readonly id: string;
        readonly choices: ReadonlyArray<string>;
        readonly allowFreeform: boolean;
        readonly resolve: (value: unknown) => void;
      };
  request: OrchestrationV2RuntimeRequest;
  node: OrchestrationV2ExecutionNode;
  item: OrchestrationV2TurnItem;
}

const nativeRef = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
  driver: COPILOT_PROVIDER,
  nativeId,
  strength,
});

const asRecord = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const asString = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;
const asNumber = (value: unknown): number => (typeof value === "number" ? value : 0);

/** T3's "default" placeholder asks Copilot to pick its own default model. */
const copilotModelId = (selection: ModelSelection): string | undefined =>
  selection.model === "default" ? undefined : selection.model;

const USAGE_LIMIT = /quota|rate.?limit|premium request|usage limit|too many requests/i;

export const makeCopilotSdkAdapterV2 = Effect.fn("makeCopilotSdkAdapterV2")(function* (
  options: CopilotSdkAdapterV2Options,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const providerHost = yield* ProviderHost.ProviderHost;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const random = yield* SecureRandom.SecureRandom;
  const platform = yield* HostProcess.Platform;

  const protocolError = (detail: string, cause?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: COPILOT_PROVIDER,
      detail,
      ...(cause === undefined ? {} : { cause }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: COPILOT_PROVIDER,
    getCapabilities: Effect.fn("CopilotSdkAdapter.getCapabilities")(function () {
      return Effect.succeed(CopilotSdkCapabilitiesV2);
    }),
    planSelectionTransition: Effect.fn("CopilotSdkAdapter.planSelectionTransition")(function () {
      return Effect.succeed(turnScopedSelectionTransition());
    }),
    openSession: Effect.fn("CopilotSdkAdapter.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? providerHost.paths.cwd;
      const openError = (cause: unknown) =>
        new ProviderAdapter.ProviderAdapterOpenSessionError({
          driver: COPILOT_PROVIDER,
          providerSessionId: input.providerSessionId,
          cause,
        });
      const spawnInput = yield* options.resolveSpawn(cwd).pipe(Effect.mapError(openError));
      const connection = yield* Effect.tryPromise({
        try: () =>
          (options.openConnection ?? openCopilotSdkConnection)({ ...spawnInput, platform }),
        catch: openError,
      });
      yield* Effect.addFinalizer(() => Effect.promise(() => connection.close()));

      const now = yield* DateTime.now;
      let session: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: COPILOT_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: CopilotSdkCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const inbox = yield* Queue.unbounded<Inbox>();
      const commands = yield* Semaphore.make(1);
      const eventPermit = yield* Semaphore.make(1);
      const pending = new Map<RuntimeRequestId, PendingRequest>();
      const finishedTerminals = new Set<string>();
      const subagentTasks = new Map<string, string>();
      // A subagent outlives the turn that started it, so its later events publish against that turn.
      const observedSubagents = new Map<string, ActiveTurn>();
      let closed = false;
      let nativeSessionId: string | undefined;
      let thread: OrchestrationV2ProviderThread | undefined;
      let active: ActiveTurn | undefined;
      let lastProviderTurn: OrchestrationV2ProviderTurn | undefined;
      let wake: { readonly nativeId: string; readonly events: Array<SdkEvent> } | undefined;
      let desiredPermission: CopilotPermissionMode = copilotPermissionModeForRuntimeMode(
        input.runtimePolicy.runtimeMode,
      );
      let appliedPermission: CopilotPermissionMode | undefined;
      let appliedSessionMode: CopilotSessionMode | undefined;
      let runtimeMode: RuntimeMode = input.runtimePolicy.runtimeMode;
      let pendingTasks: ReadonlyArray<{
        readonly taskId: string;
        readonly kind: "subagent" | "background_task";
        readonly description?: string;
      }> = [];

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const request = (
        method: string,
        params: Record<string, unknown> = {},
        timeoutMs = options.requestTimeoutMs ?? 30_000,
      ) =>
        Effect.tryPromise({
          try: () =>
            connection.request(
              method,
              { ...(nativeSessionId ? { sessionId: nativeSessionId } : {}), ...params },
              { timeoutMs },
            ),
          catch: (cause) =>
            protocolError(
              cause instanceof Error ? cause.message : `Copilot ${method} failed`,
              cause,
            ),
        });

      const updateSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = null,
      ) {
        session = {
          ...session,
          status,
          lastError:
            lastError === null ? null : makeProviderFailure({ message: lastError }).message,
          updatedAt: yield* DateTime.now,
        };
        yield* emit({
          type: "provider_session.updated",
          driver: COPILOT_PROVIDER,
          providerSession: session,
        });
      });
      const updateThread = Effect.fnUntraced(function* (
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        if (!thread) return;
        thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_thread.updated",
          driver: COPILOT_PROVIDER,
          providerThread: thread,
        });
      });

      // Permission mode and agent mode are independent in Copilot, so leaving plan mode never
      // lowers permissions; the guard below still re-asserts the requested mode after any change.
      const applyPermissionMode = Effect.fnUntraced(function* () {
        if (appliedPermission === desiredPermission) return;
        yield* request("session.permissions.setMode", { mode: desiredPermission });
        appliedPermission = desiredPermission;
      });
      const applySessionMode = Effect.fnUntraced(function* (mode: CopilotSessionMode) {
        if (appliedSessionMode === mode) return;
        yield* request("session.mode.set", { mode });
        appliedSessionMode = mode;
      });

      const itemIdentity = (turn: ActiveTurn, nativeId: string) =>
        `${options.instanceId}:${turn.input.providerThread.id}:${turn.nativeId}:${nativeId}`;
      const baseItem = (turn: ActiveTurn, nativeId: string, time: DateTime.Utc) => {
        if (!turn.ordinals.has(nativeId)) turn.ordinals.set(nativeId, turn.nextOrdinal++);
        if (!turn.started.has(nativeId)) turn.started.set(nativeId, time);
        const identity = itemIdentity(turn, nativeId);
        return {
          id: idAllocator.derive.turnItemFromProviderItem({
            driver: COPILOT_PROVIDER,
            nativeItemId: identity,
          }),
          nodeId: idAllocator.derive.nodeFromProviderItem({
            driver: COPILOT_PROVIDER,
            nativeItemId: identity,
          }),
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          providerThreadId: turn.input.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef(nativeId),
          parentItemId: null,
          ordinal: turn.ordinals.get(nativeId)!,
          startedAt: turn.started.get(nativeId)!,
          updatedAt: time,
        };
      };

      const publishItem = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        nativeId: string,
        state: TurnItemState,
        terminal?: OrchestrationV2TurnItemStatus,
      ) {
        const time = yield* DateTime.now;
        const status = terminal ?? state.status;
        const streaming = status === "running";
        const base = {
          ...baseItem(turn, nativeId, time),
          status,
          completedAt: streaming ? null : time,
          title: state.toolTitle ?? state.toolName ?? null,
        };
        let turnItem: OrchestrationV2TurnItem;
        let nodeKind: OrchestrationV2ExecutionNode["kind"] = "tool_call";
        if (state.kind === "assistant") {
          nodeKind = "assistant_message";
          const messageId = idAllocator.derive.messageFromProviderItem({
            driver: COPILOT_PROVIDER,
            nativeItemId: itemIdentity(turn, nativeId),
          });
          const message: OrchestrationV2ConversationMessage = {
            id: messageId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            nodeId: base.nodeId,
            role: "assistant",
            text: state.text,
            attachments: [],
            streaming,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: base.startedAt,
            updatedAt: time,
          };
          yield* emit({ type: "message.updated", driver: COPILOT_PROVIDER, message });
          turnItem = {
            ...base,
            type: "assistant_message",
            messageId,
            text: state.text,
            streaming,
          };
        } else if (state.kind === "reasoning") {
          nodeKind = "reasoning";
          turnItem = { ...base, type: "reasoning", text: state.text, streaming };
        } else if (state.kind === "subagent") {
          nodeKind = "subagent";
          const subagent: OrchestrationV2Subagent = {
            id: base.nodeId,
            threadId: base.threadId,
            runId: base.runId,
            parentNodeId: turn.input.rootNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver: COPILOT_PROVIDER,
            providerInstanceId: options.instanceId,
            providerThreadId: base.providerThreadId,
            childThreadId: null,
            nativeTaskRef: nativeRef(nativeId),
            prompt: state.prompt ?? "",
            title: state.agentName ?? "Copilot agent",
            model: null,
            status,
            result: state.text || null,
            startedAt: base.startedAt,
            completedAt: base.completedAt,
            updatedAt: time,
          };
          yield* emit({ type: "subagent.updated", driver: COPILOT_PROVIDER, subagent });
          turnItem = {
            ...base,
            title: subagent.title,
            type: "subagent",
            subagentId: subagent.id,
            origin: "provider_native",
            driver: COPILOT_PROVIDER,
            providerInstanceId: options.instanceId,
            childThreadId: null,
            prompt: subagent.prompt,
            result: subagent.result,
          };
        } else {
          turnItem = {
            ...base,
            ...copilotToolPresentation({
              toolName: state.toolName ?? "tool",
              title: state.toolTitle,
              args: state.args,
              output: state.text,
              status,
              exitCode: state.exitCode,
            }),
          };
        }
        yield* emit({
          type: "node.updated",
          driver: COPILOT_PROVIDER,
          node: {
            id: base.nodeId,
            threadId: base.threadId,
            runId: base.runId,
            parentNodeId: turn.input.rootNodeId,
            rootNodeId: turn.input.rootNodeId,
            kind: nodeKind,
            status,
            countsForRun: false,
            providerThreadId: base.providerThreadId,
            providerTurnId: base.providerTurnId,
            nativeItemRef: base.nativeItemRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: base.startedAt,
            completedAt: base.completedAt,
          },
        });
        yield* emit({ type: "turn_item.updated", driver: COPILOT_PROVIDER, turnItem });
      });

      const resolvePending = Effect.fnUntraced(function* (
        entry: PendingRequest,
        status: "resolved" | "cancelled",
        decision?: ProviderApprovalDecision,
      ) {
        const time = yield* DateTime.now;
        entry.request = {
          ...entry.request,
          ...(decision === undefined ? {} : { decision }),
          status,
          resolvedAt: time,
          responseCapability: { type: "not_resumable", reason: "This Copilot request has ended." },
        };
        entry.node = {
          ...entry.node,
          status: status === "resolved" ? "completed" : "cancelled",
          completedAt: time,
        };
        entry.item = {
          ...entry.item,
          status: status === "resolved" ? "completed" : "cancelled",
          completedAt: time,
          updatedAt: time,
        };
        pending.delete(entry.request.id);
        yield* emit({
          type: "runtime_request.updated",
          driver: COPILOT_PROVIDER,
          threadId: input.threadId,
          runtimeRequest: entry.request,
        });
        yield* emit({ type: "node.updated", driver: COPILOT_PROVIDER, node: entry.node });
        yield* emit({ type: "turn_item.updated", driver: COPILOT_PROVIDER, turnItem: entry.item });
      });

      const cancelPending = Effect.fnUntraced(function* () {
        for (const entry of pending.values()) {
          // A question holds a live server request open; answer it so Copilot unblocks.
          if (entry.native.type === "question")
            entry.native.resolve({ answer: "", wasFreeform: true });
          yield* resolvePending(entry, "cancelled");
        }
      });

      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "completed" | "cancelled" | "interrupted" | "failed",
        detail?: string,
        disposition: "reusable" | "broken" = "reusable",
      ) {
        if (active !== turn || finishedTerminals.has(turn.nativeId)) return;
        finishedTerminals.add(turn.nativeId);
        const completedAt = yield* DateTime.now;
        for (const [nativeId, state] of turn.items) {
          // Background agents keep running after the main loop idles, is interrupted, or is cancelled.
          if (
            state.kind === "subagent" &&
            state.status === "running" &&
            status !== "failed" &&
            disposition === "reusable"
          )
            continue;
          if (turn.dirty.has(nativeId) || state.status === "running")
            yield* publishItem(
              turn,
              nativeId,
              state,
              state.status === "running"
                ? status === "completed"
                  ? "completed"
                  : status
                : undefined,
            );
        }
        turn.dirty.clear();
        yield* cancelPending();
        const usage = turn.usage;
        turn.providerTurn = {
          ...turn.providerTurn,
          status,
          completedAt,
          ...(usage.inputTokens + usage.outputTokens > 0
            ? {
                turnTokenUsage: {
                  usageScope: "main_agent",
                  usageStatus: "complete",
                  hasSubagents: turn.hasSubagents,
                  inputTokens: usage.inputTokens,
                  outputTokens: usage.outputTokens,
                  cachedInputTokens: usage.cacheReadTokens,
                  reasoningTokens: usage.reasoningTokens,
                  ...(usage.cacheWriteTokens > 0
                    ? { cacheCreationTokens: usage.cacheWriteTokens }
                    : {}),
                },
              }
            : {}),
        };
        lastProviderTurn = turn.providerTurn;
        yield* emit({
          type: "provider_turn.updated",
          driver: COPILOT_PROVIDER,
          threadId: turn.input.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateThread({
          status: disposition === "broken" ? "error" : "idle",
          ...(!turn.compact && status === "completed"
            ? { nativeConversationHeadRef: nativeRef(turn.nativeId, "weak") }
            : {}),
        });
        yield* updateSession(disposition === "broken" ? "error" : "ready", detail ?? null);
        active = undefined;
        if (status === "failed") {
          const usageLimited = USAGE_LIMIT.test(detail ?? "");
          const failure = makeProviderFailure({
            class:
              disposition === "broken"
                ? "transport_error"
                : usageLimited
                  ? "usage_limit"
                  : "provider_error",
            message: detail ?? "Copilot turn failed.",
          });
          const base = baseItem(turn, `failure:${turn.nativeId}`, completedAt);
          yield* emit({
            type: "turn_item.updated",
            driver: COPILOT_PROVIDER,
            turnItem: {
              ...base,
              type: "error",
              status: "failed",
              title: usageLimited ? "Usage limit reached" : null,
              completedAt,
              failure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: COPILOT_PROVIDER,
            providerThreadId: turn.providerTurn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure,
            failureItemOrdinal: base.ordinal,
            threadDisposition: disposition,
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: COPILOT_PROVIDER,
            providerThreadId: turn.providerTurn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure: null,
            threadDisposition: disposition,
          });
        }
        yield* Deferred.succeed(turn.done, undefined);
      });

      const refreshTasks = Effect.fnUntraced(function* () {
        if (!nativeSessionId || closed) return;
        const result = yield* request("session.tasks.list").pipe(Effect.option);
        if (result._tag === "None") return;
        const tasks = Array.isArray(asRecord(result.value).tasks)
          ? (asRecord(result.value).tasks as Array<unknown>)
          : [];
        subagentTasks.clear();
        pendingTasks = tasks.flatMap((raw) => {
          const task = asRecord(raw);
          const id = asString(task.id);
          const type = asString(task.type);
          const status = asString(task.status);
          if (!id) return [];
          const toolCallId = asString(task.toolCallId);
          if (toolCallId) subagentTasks.set(toolCallId, id);
          if (status !== "running" && status !== "idle") return [];
          const description =
            asString(task.displayName) ?? asString(task.description) ?? asString(task.command);
          return [
            {
              taskId: id,
              kind: type === "agent" ? ("subagent" as const) : ("background_task" as const),
              ...(description ? { description } : {}),
            },
          ];
        });
        yield* updateThread({ pendingBackgroundTasks: pendingTasks });
      });

      const publishRequest = Effect.fnUntraced(function* (native: PendingRequest["native"]) {
        const turn = active;
        if (!thread) return;
        const providerTurnId = turn?.providerTurn.id ?? null;
        const requestId = yield* idAllocator.allocate
          .runtimeRequest({
            driver: COPILOT_PROVIDER,
            ...(providerTurnId ? { providerTurnId } : {}),
            nativeRequestId: native.id,
          })
          .pipe(
            Effect.mapError((cause) => protocolError("Cannot allocate Copilot request", cause)),
          );
        const time = yield* DateTime.now;
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const kind =
          native.type === "question"
            ? ("user_input" as const)
            : copilotPermissionKind(native.request);
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId,
          nativeRequestRef: nativeRef(native.id),
          kind,
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt: time,
          resolvedAt: null,
        };
        const threadId = turn?.input.threadId ?? thread.appThreadId ?? input.threadId;
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId,
          runId: turn?.input.runId ?? null,
          parentNodeId: turn?.input.rootNodeId ?? null,
          rootNodeId: turn?.input.rootNodeId ?? nodeId,
          kind: native.type === "permission" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId: thread.id,
          providerTurnId,
          nativeItemRef: nativeRef(native.id),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: time,
          completedAt: null,
        };
        const latest = active?.providerTurn ?? lastProviderTurn;
        const base = {
          ...(turn
            ? baseItem(turn, `request:${native.id}`, time)
            : {
                threadId,
                runId: null,
                providerThreadId: thread.id,
                providerTurnId: null,
                nativeItemRef: nativeRef(native.id),
                parentItemId: null,
                ordinal: ((latest?.ordinal ?? 0) + 1) * 100,
                startedAt: time,
                updatedAt: time,
              }),
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          nodeId,
          status: "waiting" as const,
          title: null,
          completedAt: null,
        };
        const item: OrchestrationV2TurnItem =
          native.type === "permission"
            ? {
                ...base,
                type: "approval_request",
                requestId,
                requestKind: kind === "user_input" ? "command" : kind,
                prompt: copilotPermissionPrompt(native.request),
                options: copilotApprovalOptions(native.request),
              }
            : {
                ...base,
                type: "user_input_request",
                requestId,
                questions: [
                  {
                    id: "answer",
                    header: "Question",
                    question: native.id,
                    options: native.choices.map((label) => ({ label, description: label })),
                    multiSelect: false,
                    allowCustomAnswer: native.allowFreeform,
                    required: true,
                  },
                ],
              };
        const entry: PendingRequest = { native, request: runtimeRequest, node, item };
        pending.set(requestId, entry);
        yield* emit({
          type: "runtime_request.updated",
          driver: COPILOT_PROVIDER,
          threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: COPILOT_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: COPILOT_PROVIDER, turnItem: item });
        yield* updateSession("waiting");
      });

      const publishProposedPlan = Effect.fnUntraced(function* (turn: ActiveTurn, markdown: string) {
        const nativeItemId = `${turn.nativeId}:proposed-plan`;
        const time = yield* DateTime.now;
        const planId = yield* idAllocator.allocate
          .plan({
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            driver: COPILOT_PROVIDER,
          })
          .pipe(Effect.orDie);
        const nodeId = idAllocator.derive.nodeFromProviderItem({
          driver: COPILOT_PROVIDER,
          nativeItemId,
        });
        const ordinal = turn.nextOrdinal++;
        yield* emit({
          type: "node.updated",
          driver: COPILOT_PROVIDER,
          node: {
            id: nodeId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            parentNodeId: turn.input.rootNodeId,
            rootNodeId: turn.input.rootNodeId,
            kind: "plan",
            status: "completed",
            countsForRun: false,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: nativeRef(nativeItemId, "weak"),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: time,
            completedAt: time,
          },
        });
        yield* emit({
          type: "plan.updated",
          driver: COPILOT_PROVIDER,
          plan: {
            id: planId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            nodeId,
            kind: "proposed_plan",
            status: "completed",
            markdown,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: COPILOT_PROVIDER,
          turnItem: {
            id: idAllocator.derive.turnItemFromProviderItem({
              driver: COPILOT_PROVIDER,
              nativeItemId,
            }),
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            nodeId,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: nativeRef(nativeItemId, "weak"),
            parentItemId: null,
            ordinal,
            status: "completed",
            title: null,
            startedAt: time,
            completedAt: time,
            updatedAt: time,
            type: "proposed_plan",
            planId,
            markdown,
            streaming: false,
          },
        });
      });

      const touch = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        nativeId: string,
        state: TurnItemState,
        immediate = false,
      ) {
        turn.items.set(nativeId, state);
        if (immediate || state.status !== "running") {
          turn.dirty.delete(nativeId);
          yield* publishItem(turn, nativeId, state);
          return;
        }
        turn.dirty.add(nativeId);
        if (!turn.flushScheduled) {
          turn.flushScheduled = true;
          // Streaming deltas arrive per token; publish at most once per interval.
          yield* Effect.sleep("40 millis").pipe(
            Effect.andThen(Queue.offer(inbox, { type: "flush", turn })),
            Effect.forkIn(scope),
          );
        }
      });

      const handleTurnEvent = Effect.fnUntraced(function* (turn: ActiveTurn, event: SdkEvent) {
        const data = event.data ?? {};
        const isSub = event.agentId !== undefined || data.parentToolCallId !== undefined;
        switch (event.type) {
          case "assistant.message_delta": {
            if (isSub) return;
            const id = asString(data.messageId);
            if (!id) return;
            const state = turn.items.get(id) ?? { kind: "assistant", text: "", status: "running" };
            yield* touch(turn, id, {
              ...state,
              text: state.text + (asString(data.deltaContent) ?? ""),
            });
            return;
          }
          case "assistant.message": {
            if (isSub) return;
            const id = asString(data.messageId);
            const content = asString(data.content) ?? "";
            if (!id || !content.trim()) return;
            yield* touch(turn, id, { kind: "assistant", text: content, status: "completed" });
            return;
          }
          case "assistant.reasoning_delta": {
            const id = asString(data.reasoningId);
            if (!id || isSub) return;
            const state = turn.items.get(id) ?? { kind: "reasoning", text: "", status: "running" };
            yield* touch(turn, id, {
              ...state,
              text: state.text + (asString(data.deltaContent) ?? ""),
            });
            return;
          }
          case "assistant.reasoning": {
            const id = asString(data.reasoningId);
            const content = asString(data.content) ?? "";
            if (!id || isSub || !content.trim()) return;
            yield* touch(turn, id, { kind: "reasoning", text: content, status: "completed" });
            return;
          }
          case "tool.execution_start": {
            const toolCallId = asString(data.toolCallId);
            const toolName = asString(data.toolName) ?? "tool";
            if (!toolCallId || isSub) return;
            // Subagent spawns, intent reports and plan exits have dedicated rows.
            if (["task", "report_intent", "exit_plan_mode", "ask_user"].includes(toolName)) return;
            yield* touch(
              turn,
              toolCallId,
              {
                kind: "tool",
                text: "",
                status: "running",
                toolName,
                ...(asString(data.toolTitle) ? { toolTitle: asString(data.toolTitle)! } : {}),
                args: asRecord(data.arguments),
              },
              true,
            );
            return;
          }
          case "tool.shell_output": {
            const toolCallId = asString(data.toolCallId);
            const text = asString(data.text);
            if (!toolCallId || !text || isSub) return;
            const state = turn.items.get(toolCallId);
            // Output of a shell from an earlier turn has no row in this one.
            if (!state) return;
            state.text += text;
            yield* emit({
              type: "command_output.delta",
              driver: COPILOT_PROVIDER,
              threadId: turn.input.threadId,
              itemId: baseItem(turn, toolCallId, yield* DateTime.now).id,
              chunk: text,
            });
            return;
          }
          case "tool.execution_complete": {
            const toolCallId = asString(data.toolCallId);
            const state = toolCallId ? turn.items.get(toolCallId) : undefined;
            if (!toolCallId || !state || state.kind !== "tool") return;
            const result = asRecord(data.result);
            const error = asRecord(data.error);
            const exitCode = asRecord(data.shellExecution).exitCode;
            const output =
              asString(result.detailedContent) ??
              asString(result.content) ??
              asString(error.message) ??
              state.text;
            yield* touch(turn, toolCallId, {
              ...state,
              text: output || state.text,
              status: data.success === false ? "failed" : "completed",
              ...(typeof exitCode === "number" ? { exitCode } : {}),
            });
            return;
          }
          case "subagent.started": {
            const toolCallId = asString(data.toolCallId);
            if (!toolCallId) return;
            turn.hasSubagents = true;
            observedSubagents.set(toolCallId, turn);
            yield* touch(
              turn,
              toolCallId,
              {
                kind: "subagent",
                text: "",
                status: "running",
                agentName: asString(data.agentDisplayName) ?? asString(data.agentName),
                prompt: asString(data.agentDescription) ?? "",
              },
              true,
            );
            yield* refreshTasks();
            return;
          }
          case "subagent.completed":
          case "subagent.failed": {
            const toolCallId = asString(data.toolCallId);
            const state = toolCallId ? turn.items.get(toolCallId) : undefined;
            if (!toolCallId || !state) return;
            observedSubagents.delete(toolCallId);
            yield* touch(turn, toolCallId, {
              ...state,
              status:
                event.type === "subagent.failed"
                  ? "failed"
                  : data.cancelled === true
                    ? "cancelled"
                    : "completed",
              text: asString(data.error) ?? state.text,
            });
            yield* refreshTasks();
            return;
          }
          case "assistant.usage": {
            const usage = turn.usage;
            if (isSub) {
              turn.hasSubagents = true;
              return;
            }
            // One event per model call, so the sum is this turn's usage, not the session's.
            usage.inputTokens += asNumber(data.inputTokens);
            usage.outputTokens += asNumber(data.outputTokens);
            usage.cacheReadTokens += asNumber(data.cacheReadTokens);
            usage.cacheWriteTokens += asNumber(data.cacheWriteTokens);
            usage.reasoningTokens += asNumber(data.reasoningTokens);
            return;
          }
          case "session.usage_info": {
            const used = asNumber(data.currentTokens);
            const limit = asNumber(data.tokenLimit);
            if (used > 0)
              yield* updateThread({
                contextUsage: {
                  usedTokens: used,
                  ...(limit > 0 ? { maxTokens: limit } : {}),
                },
              });
            return;
          }
          case "abort":
            turn.aborted = true;
            return;
          case "session.error": {
            turn.failure = asString(data.message) ?? "Copilot reported an error.";
            yield* finish(turn, "failed", turn.failure);
            return;
          }
          case "assistant.idle":
          case "session.idle": {
            if (turn.compact) return;
            yield* refreshTasks();
            yield* finish(
              turn,
              turn.interruptRequested || turn.aborted || data.aborted === true
                ? "interrupted"
                : "completed",
            );
            return;
          }
          case "session.background_tasks_changed":
            yield* refreshTasks();
            return;
          default:
            return;
        }
      });

      const handleEvent = Effect.fnUntraced(function* (event: SdkEvent) {
        const data = event.data ?? {};
        switch (event.type) {
          case "permission.requested": {
            if (data.resolvedByHook === true) return;
            const requestId = asString(data.requestId);
            if (!requestId) return;
            const permissionRequest = asRecord(data.permissionRequest) as CopilotPermissionRequest;
            // Copilot has no "accept edits" posture; T3's mode answers write requests itself.
            if (runtimeMode === "auto-accept-edits" && permissionRequest.kind === "write") {
              yield* request("session.permissions.handlePendingPermissionRequest", {
                requestId,
                result: { kind: "approve-once" },
              }).pipe(Effect.ignore);
              return;
            }
            yield* publishRequest({
              type: "permission",
              request: permissionRequest,
              id: requestId,
            });
            return;
          }
          case "permission.completed": {
            const requestId = asString(data.requestId);
            const entry = [...pending.values()].find(
              (candidate) =>
                candidate.native.type === "permission" && candidate.native.id === requestId,
            );
            if (entry) yield* resolvePending(entry, "resolved");
            if (pending.size === 0 && active) yield* updateSession("running");
            return;
          }
          case "session.mode_changed": {
            appliedSessionMode =
              (asString(data.newMode) as CopilotSessionMode | undefined) ?? appliedSessionMode;
            // Leaving plan mode must not lower permissions below what was requested.
            appliedPermission = undefined;
            yield* applyPermissionMode().pipe(Effect.ignore);
            return;
          }
          default:
            break;
        }
        if (event.type === "subagent.completed" || event.type === "subagent.failed") {
          const owner = observedSubagents.get(asString(data.toolCallId) ?? "");
          if (owner && owner !== active) return yield* handleTurnEvent(owner, event);
        }
        const turn = active;
        if (!turn) {
          // Background agents finishing wake the main agent without a T3 turn: hold the events
          // and ask the orchestrator to start the continuation that will adopt them.
          if (event.type === "assistant.turn_start" && event.agentId === undefined && !wake) {
            const held = { nativeId: `wake:${yield* random.uuidV4()}`, events: [event] };
            wake = held;
            if (thread?.appThreadId && options.continuationRequests)
              yield* options.continuationRequests.offer({
                threadId: thread.appThreadId,
                providerThreadId: thread.id,
                driver: COPILOT_PROVIDER,
                detail: null,
                dispatchIfCurrent: (dispatch) =>
                  wake === held ? Effect.asSome(dispatch) : Effect.succeedNone,
                clearIfCurrent: () =>
                  Effect.sync(() => {
                    if (wake === held) wake = undefined;
                  }),
              });
          } else if (wake) wake.events.push(event);
          else if (event.type === "session.background_tasks_changed") yield* refreshTasks();
          return;
        }
        yield* handleTurnEvent(turn, event);
      });

      const failConnection = Effect.fnUntraced(function* (detail: string) {
        if (closed) return;
        closed = true;
        const turn = active;
        if (turn) yield* finish(turn, "failed", detail, "broken");
        else {
          yield* updateThread({ status: "error" });
          yield* updateSession("error", detail);
        }
        for (const [toolCallId, owner] of observedSubagents) {
          const state = owner.items.get(toolCallId);
          if (state) yield* publishItem(owner, toolCallId, state, "failed");
        }
        observedSubagents.clear();
        yield* cancelPending();
        yield* Queue.fail(events, protocolError(detail));
      });

      connection.onNotification((method, params) => {
        if (method !== "session.event") return;
        const body = asRecord(params);
        const event = asRecord(body.event);
        // One server can host several sessions; this runtime owns exactly one.
        if (asString(body.sessionId) !== nativeSessionId || !asString(event.type)) return;
        Queue.offerUnsafe(inbox, { type: "event", event: event as unknown as SdkEvent });
      });
      connection.onRequest(
        "userInput.request",
        (params) =>
          new Promise((resolve) => {
            Queue.offerUnsafe(inbox, { type: "userInput", params: asRecord(params), resolve });
          }),
      );
      connection.onRequest(
        "exitPlanMode.request",
        (params) =>
          new Promise((resolve) => {
            Queue.offerUnsafe(inbox, { type: "exitPlan", params: asRecord(params), resolve });
          }),
      );
      connection.onClose((info) => {
        if (closed) return;
        Queue.offerUnsafe(inbox, {
          type: "closed",
          detail: info.stderr || `Copilot SDK server exited with code ${info.code ?? "unknown"}`,
        });
      });

      yield* Stream.fromQueue(inbox).pipe(
        Stream.runForEach((entry) =>
          eventPermit.withPermits(1)(
            Effect.gen(function* () {
              switch (entry.type) {
                case "event":
                  return yield* handleEvent(entry.event);
                case "flush": {
                  entry.turn.flushScheduled = false;
                  for (const nativeId of entry.turn.dirty) {
                    entry.turn.dirty.delete(nativeId);
                    const state = entry.turn.items.get(nativeId);
                    if (state && active === entry.turn)
                      yield* publishItem(entry.turn, nativeId, state);
                  }
                  return;
                }
                case "closed":
                  return yield* failConnection(entry.detail);
                case "userInput": {
                  const id =
                    asString(entry.params.requestId) ??
                    asString(entry.params.question) ??
                    "question";
                  const choices = Array.isArray(entry.params.choices)
                    ? (entry.params.choices as Array<unknown>).filter(
                        (choice): choice is string => typeof choice === "string",
                      )
                    : [];
                  return yield* publishRequest({
                    type: "question",
                    id: `${id}`,
                    choices,
                    allowFreeform: entry.params.allowFreeform !== false,
                    resolve: entry.resolve,
                  });
                }
                case "exitPlan": {
                  const markdown =
                    asString(entry.params.planContent) ?? asString(entry.params.summary) ?? "";
                  if (active && markdown.trim()) yield* publishProposedPlan(active, markdown);
                  // T3 owns "implement this plan": leave plan mode, run nothing further.
                  entry.resolve({ approved: true, selectedAction: "exit_only" });
                  return;
                }
              }
            }).pipe(
              Effect.catch((error) =>
                Effect.logWarning("copilot-sdk.event-failed", {
                  detail: error instanceof Error ? error.message : String(error),
                }),
              ),
            ),
          ),
        ),
        Effect.forkIn(scope),
      );

      const sessionBody = Effect.fnUntraced(function* (
        args: Pick<
          ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
          "threadId" | "modelSelection"
        >,
      ) {
        const mcpSession = yield* mcpSessions.read(args.threadId);
        const effort = getModelSelectionStringOptionValue(args.modelSelection, "reasoningEffort");
        const modelId = copilotModelId(args.modelSelection);
        return {
          ...(modelId ? { model: modelId } : {}),
          ...(effort ? { reasoningEffort: effort } : {}),
          workingDirectory: cwd,
          streaming: true,
          clientName: "t3code",
          requestPermission: true,
          requestUserInput: true,
          requestExitPlanMode: true,
          includeSubAgentStreamingEvents: true,
          ...(mcpSession
            ? {
                mcpServers: {
                  "t3-code": {
                    type: "http",
                    url: mcpSession.endpoint,
                    headers: { Authorization: mcpSession.authorizationHeader },
                    tools: ["*"],
                  },
                },
              }
            : {}),
        };
      });

      const register = Effect.fnUntraced(function* (
        args: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        const existing = args.existingProviderThread;
        const requestedId =
          args.forceNewNativeThread === true
            ? undefined
            : (existing?.nativeThreadRef?.nativeId ?? input.initialNativeThreadId);
        nativeSessionId = requestedId ?? (yield* random.uuidV4());
        const body = yield* sessionBody(args);
        const result = yield* request(requestedId ? "session.resume" : "session.create", {
          ...body,
          sessionId: nativeSessionId,
        });
        const returned = asString(asRecord(result).sessionId);
        if (returned && returned !== nativeSessionId)
          return yield* protocolError("Copilot returned a different session identity");
        runtimeMode = args.runtimePolicy.runtimeMode;
        desiredPermission = copilotPermissionModeForRuntimeMode(runtimeMode);
        appliedPermission = undefined;
        appliedSessionMode = undefined;
        yield* applyPermissionMode();
        const createdAt = yield* DateTime.now;
        thread = existing
          ? {
              ...existing,
              providerSessionId: input.providerSessionId,
              nativeThreadRef: nativeRef(nativeSessionId),
              status: "idle",
              updatedAt: createdAt,
            }
          : {
              id: idAllocator.derive.providerThread({
                driver: COPILOT_PROVIDER,
                providerInstanceId: options.instanceId,
                nativeThreadId: nativeSessionId,
              }),
              driver: COPILOT_PROVIDER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: args.threadId,
              ownerNodeId: null,
              nativeThreadRef: nativeRef(nativeSessionId),
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt,
              updatedAt: createdAt,
            };
        session = { ...session, model: args.modelSelection.model };
        yield* emit({
          type: "provider_thread.updated",
          driver: COPILOT_PROVIDER,
          providerThread: thread,
        });
        return thread;
      });

      const validateThread = (candidate: OrchestrationV2ProviderThread) =>
        thread !== undefined &&
        candidate.driver === COPILOT_PROVIDER &&
        candidate.providerInstanceId === options.instanceId &&
        candidate.nativeThreadRef?.nativeId === nativeSessionId;

      const promptText = Effect.fnUntraced(function* (
        message: ProviderAdapter.ProviderAdapterV2TurnMessage,
      ) {
        const text = yield* providerMessageTextWithAttachmentPaths({
          text: message.text,
          attachments: message.attachments,
          resolveAttachmentPath: providerHost.resolveAttachmentPath,
          resolveExtractedTextPath: providerHost.resolveExtractedTextPath,
        });
        if (!text.trim()) return yield* protocolError("Copilot needs a text message");
        return text;
      });

      const start = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
        compact = false,
      ) {
        if (!validateThread(turnInput.providerThread) || closed)
          return yield* protocolError("Copilot thread is not attached to this server");
        if (active)
          return yield* new ProviderAdapter.ProviderAdapterProtocolError({
            driver: COPILOT_PROVIDER,
            detail:
              "Copilot already has an active turn; steer it or queue through the orchestrator",
            reason: "previous_turn_active",
          });
        const continuation =
          turnInput.message.createdBy === "agent" &&
          turnInput.message.creationSource === "provider";
        // Mode changes land before the turn so a bypass selection holds from its first tool call.
        runtimeMode = turnInput.runtimePolicy.runtimeMode;
        desiredPermission = copilotPermissionModeForRuntimeMode(runtimeMode);
        yield* applyPermissionMode();
        yield* applySessionMode(
          turnInput.runtimePolicy.interactionMode === "plan" ? "plan" : "interactive",
        );
        const targetModel = copilotModelId(turnInput.modelSelection);
        if (!continuation && targetModel && session.model !== targetModel) {
          const effort = getModelSelectionStringOptionValue(
            turnInput.modelSelection,
            "reasoningEffort",
          );
          yield* request("session.model.switchTo", {
            modelId: targetModel,
            ...(effort ? { reasoningEffort: effort } : {}),
          });
          session = { ...session, model: targetModel };
        }
        const text = compact || continuation ? "" : yield* promptText(turnInput.message);
        const held = wake;
        wake = undefined;
        const nativeId = held?.nativeId ?? (yield* random.uuidV4());
        const startedAt = yield* DateTime.now;
        const providerTurn: OrchestrationV2ProviderTurn = {
          id: idAllocator.derive.providerTurn({
            driver: COPILOT_PROVIDER,
            nativeTurnId: `${options.instanceId}:${nativeSessionId}:${nativeId}`,
          }),
          providerThreadId: turnInput.providerThread.id,
          nodeId: turnInput.rootNodeId,
          runAttemptId: turnInput.attemptId,
          nativeTurnRef: nativeRef(nativeId, "weak"),
          ordinal: turnInput.providerTurnOrdinal,
          status: "running",
          startedAt,
          completedAt: null,
        };
        const turn: ActiveTurn = {
          input: turnInput,
          providerTurn,
          nativeId,
          items: new Map(),
          ordinals: new Map(),
          started: new Map(),
          dirty: new Set(),
          done: yield* Deferred.make<void>(),
          usage: {
            inputTokens: 0,
            outputTokens: 0,
            cacheReadTokens: 0,
            cacheWriteTokens: 0,
            reasoningTokens: 0,
          },
          nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
          flushScheduled: false,
          interruptRequested: false,
          aborted: false,
          failure: undefined,
          compact,
          hasSubagents: false,
          steers: 0,
        };
        yield* Effect.gen(function* () {
          thread = turnInput.providerThread;
          active = turn;
          yield* emit({
            type: "provider_turn.updated",
            driver: COPILOT_PROVIDER,
            threadId: turnInput.threadId,
            providerTurn,
          });
          yield* updateThread({
            status: "active",
            firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
          });
          yield* updateSession("running");
          for (const event of held?.events ?? []) yield* handleEvent(event);
        }).pipe(eventPermit.withPermits(1));
        if (continuation) {
          if (!held) yield* eventPermit.withPermits(1)(finish(turn, "completed"));
          return;
        }
        yield* Effect.gen(function* () {
          if (compact) {
            const result = yield* request("session.history.compact", {}, 120_000);
            const removed = asNumber(asRecord(result).tokensRemoved);
            yield* eventPermit.withPermits(1)(
              Effect.gen(function* () {
                const time = yield* DateTime.now;
                const base = baseItem(turn, "compaction", time);
                yield* emit({
                  type: "turn_item.updated",
                  driver: COPILOT_PROVIDER,
                  turnItem: {
                    ...base,
                    type: "compaction",
                    driver: COPILOT_PROVIDER,
                    status: "completed",
                    title: null,
                    completedAt: time,
                    summary:
                      asString(asRecord(result).summaryContent) ??
                      (removed > 0
                        ? `Removed ${removed} tokens of context.`
                        : "Nothing to compact."),
                  },
                });
                yield* finish(turn, asRecord(result).success === false ? "failed" : "completed");
              }),
            );
            return;
          }
          yield* request("session.send", {
            prompt: `${buildRuntimeInstructions({ harness: "GitHub Copilot", model: session.model ?? undefined })}\n\n${text}`,
            displayPrompt: turnInput.message.text || text,
            // T3 owns turn queueing, so Copilot's own queue must never hold a turn back: it only
            // drains at session idle, which stays deferred while a detached shell or background
            // agent from an earlier turn is still running.
            mode: "immediate",
            agentMode: turnInput.runtimePolicy.interactionMode === "plan" ? "plan" : "interactive",
          });
        }).pipe(
          Effect.onError((cause) => {
            const error = Cause.squash(cause);
            const detail = isProtocolError(error)
              ? error.detail
              : error instanceof Error
                ? error.message
                : "Copilot could not start the turn.";
            return eventPermit.withPermits(1)(finish(turn, "failed", detail));
          }),
        );
      });

      const stopTaskById = Effect.fnUntraced(function* (taskId: string) {
        // Copilot cancels by its task id; callers may hold the tool call id the roster showed.
        const id = subagentTasks.get(taskId) ?? taskId;
        const result = yield* request("session.tasks.cancel", { id });
        if (asRecord(result).cancelled === false)
          return yield* protocolError("Copilot did not find a running task to stop");
        yield* refreshTasks();
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: COPILOT_PROVIDER,
        providerSessionId: input.providerSessionId,
        providerSession: session,
        events: Stream.fromQueue(events),
        backgroundTasksEndWithProcess: true,
        hasPendingBackgroundWork: Effect.sync(() => pendingTasks.length > 0 || wake !== undefined),
        ensureThread: (args) =>
          register(args).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: COPILOT_PROVIDER,
                  threadId: args.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (args) =>
          register({
            threadId: args.threadId ?? args.providerThread.appThreadId ?? input.threadId,
            modelSelection: args.modelSelection ?? input.modelSelection,
            runtimePolicy: args.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: args.providerThread,
          }).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: COPILOT_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: args.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (args) =>
          start(args).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: COPILOT_PROVIDER,
                  threadId: args.threadId,
                  providerThreadId: args.providerThread.id,
                  runId: args.runId,
                  cause,
                }),
            ),
          ),
        compactThread: (args) => start(args, true).pipe(commands.withPermits(1)),
        steerTurn: Effect.fn("CopilotSdkAdapter.steerTurn")(
          function* (args) {
            const turn = active;
            if (
              !turn ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread) ||
              turn.compact
            )
              return yield* protocolError("This Copilot turn cannot be steered");
            const text = yield* promptText(args.message);
            // `immediate` interjects into the running turn; background agents keep running.
            yield* request("session.send", {
              prompt: text,
              displayPrompt: args.message.text || text,
              mode: "immediate",
            });
            turn.steers += 1;
          },
          (effect, args) =>
            // Not under `commands`: a steer must not wait behind a command that is waiting on the turn.
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: COPILOT_PROVIDER,
                    providerThreadId: args.providerThread.id,
                    providerTurnId: args.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        stopTask: (args) =>
          stopTaskById(args.taskId).pipe(
            Effect.mapError((cause) => protocolError("Cannot stop Copilot task", cause)),
          ),
        stopSubagent: (args) =>
          stopTaskById(args.nativeTaskId).pipe(
            Effect.mapError((cause) => protocolError("Cannot stop Copilot subagent", cause)),
          ),
        interruptTurn: Effect.fn("CopilotSdkAdapter.interruptTurn")(
          function* (args) {
            const turn = active;
            if (args.requestRuntimeRestart === true) {
              yield* request("session.cancelAllBackgroundAgents").pipe(Effect.ignore);
              if (!turn) return;
            }
            if (
              !turn ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            )
              return yield* protocolError("This Copilot turn is no longer active");
            turn.interruptRequested = true;
            if (turn.compact) {
              yield* request("session.history.abortManualCompaction").pipe(Effect.ignore);
            } else {
              // Ends the main turn only; background agents are stopped separately.
              yield* request("session.interruptMainTurn", { flushQueued: false });
            }
            yield* Deferred.await(turn.done).pipe(
              Effect.timeout(options.interruptSettleMs ?? 10_000),
              Effect.catch(() => eventPermit.withPermits(1)(finish(turn, "interrupted"))),
            );
          },
          (effect, args) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: COPILOT_PROVIDER,
                    providerThreadId: args.providerThread.id,
                    providerTurnId: args.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        respondToRuntimeRequest: Effect.fn("CopilotSdkAdapter.respondToRuntimeRequest")(
          function* (args) {
            const entry = pending.get(args.requestId);
            if (!entry) return yield* protocolError("This Copilot request is no longer pending");
            if (entry.native.type === "permission") {
              const decision = args.decision;
              if (!decision) return yield* protocolError("A permission response needs a decision");
              const result = copilotPermissionDecision(
                entry.native.request,
                decision,
                args.feedback,
              );
              if (!result) return yield* protocolError("Copilot did not offer this decision");
              yield* request("session.permissions.handlePendingPermissionRequest", {
                requestId: entry.native.id,
                result,
              });
              yield* eventPermit.withPermits(1)(resolvePending(entry, "resolved", decision));
            } else {
              const raw = args.answers?.answer;
              const answer = Array.isArray(raw)
                ? raw.join("\n")
                : typeof raw === "string"
                  ? raw
                  : "";
              if (!answer.trim()) return yield* protocolError("Copilot needs a nonempty answer");
              const native = entry.native;
              entry.native.resolve({
                answer,
                wasFreeform: !native.choices.includes(answer),
              });
              yield* eventPermit.withPermits(1)(resolvePending(entry, "resolved"));
            }
            yield* eventPermit.withPermits(1)(
              Effect.suspend(() =>
                active && pending.size === 0 ? updateSession("running") : Effect.void,
              ),
            );
          },
          (effect, args) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: COPILOT_PROVIDER,
                    requestId: args.requestId,
                    cause,
                  }),
              ),
            ),
        ),
        readThreadSnapshot: (args) =>
          Effect.succeed({
            providerThread: args.providerThread,
            providerTurns: [],
            messages: [],
            runtimeRequests: [...pending.values()].map((entry) => entry.request),
          }),
        rollbackThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: COPILOT_PROVIDER,
              providerThreadId: args.providerThread.id,
              checkpointId: args.target.checkpointId,
              cause: "GitHub Copilot does not support conversation rollback in T3 Code.",
            }),
          ),
        forkThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: COPILOT_PROVIDER,
              providerThreadId: args.sourceProviderThread.id,
              cause: "GitHub Copilot does not support native forks in T3 Code.",
            }),
          ),
      };
      return runtime;
    }),
  });
});
