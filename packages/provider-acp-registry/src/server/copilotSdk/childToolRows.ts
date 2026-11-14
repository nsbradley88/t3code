/**
 * The tool calls a Copilot subagent makes, shown as rows in that subagent's child thread.
 *
 * Copilot attributes a subagent's events with `agentId` (and `parentToolCallId` on tool events), and
 * the adapter routes them to the child thread by that identity, so rows keep arriving after the
 * parent turn has ended, just as the agent's own messages do.
 */
import type {
  NodeId,
  OrchestrationV2ExecutionNode,
  OrchestrationV2TurnItem,
  OrchestrationV2TurnItemStatus,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { copilotToolPresentation } from "./protocol.ts";

/** Subagent spawns, intent reports and plan exits have dedicated rows, never tool rows. */
const NO_TOOL_ROW = new Set(["task", "report_intent", "exit_plan_mode", "ask_user"]);

const CHILD_TOOL_EVENTS: ReadonlySet<string> = new Set([
  "tool.execution_start",
  "tool.shell_output",
  "tool.execution_complete",
]);

/** The part of a child thread the rows need. */
export interface ChildThreadRef {
  readonly threadId: ThreadId;
  readonly rootNodeId: NodeId;
  readonly nativeKey: string;
  nextOrdinal: number;
}

interface ChildToolState {
  readonly nativeId: string;
  readonly ordinal: number;
  readonly startedAt: DateTime.Utc;
  toolName: string;
  toolTitle: string | undefined;
  args: Record<string, unknown> | undefined;
  text: string;
  status: OrchestrationV2TurnItemStatus;
  exitCode: number | undefined;
}

export interface ChildToolRowDeps {
  readonly driver: ProviderDriverKind;
  readonly emit: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<void>;
  readonly turnItemId: (nativeItemId: string) => TurnItemId;
  readonly nodeId: (nativeItemId: string) => NodeId;
  readonly nativeRef: (nativeId: string) => {
    readonly driver: ProviderDriverKind;
    readonly nativeId: string;
    readonly strength: "strong" | "weak";
  };
}

const record = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const text = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;

export function makeChildToolRows(deps: ChildToolRowDeps) {
  const byChild = new WeakMap<object, Map<string, ChildToolState>>();
  const toolsOf = (child: ChildThreadRef) => {
    let tools = byChild.get(child);
    if (tools === undefined) {
      tools = new Map();
      byChild.set(child, tools);
    }
    return tools;
  };
  const identity = (child: ChildThreadRef, nativeId: string) =>
    `${child.nativeKey}:tool:${nativeId}`;

  const publish = Effect.fnUntraced(function* (
    child: ChildThreadRef,
    state: ChildToolState,
    terminal?: OrchestrationV2TurnItemStatus,
  ) {
    const time = yield* DateTime.now;
    const status = terminal ?? state.status;
    const running = status === "running";
    const key = identity(child, state.nativeId);
    const nodeId = deps.nodeId(key);
    const nativeItemRef = deps.nativeRef(state.nativeId);
    const base = {
      id: deps.turnItemId(key),
      threadId: child.threadId,
      runId: null,
      nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef,
      parentItemId: null,
      ordinal: state.ordinal,
      startedAt: state.startedAt,
      updatedAt: time,
      status,
      completedAt: running ? null : time,
      title: state.toolTitle ?? state.toolName,
    };
    const turnItem: OrchestrationV2TurnItem = {
      ...base,
      ...copilotToolPresentation({
        toolName: state.toolName,
        title: state.toolTitle,
        args: state.args,
        output: state.text,
        status,
        exitCode: state.exitCode,
      }),
    };
    const node: OrchestrationV2ExecutionNode = {
      id: nodeId,
      threadId: child.threadId,
      runId: null,
      parentNodeId: child.rootNodeId,
      rootNodeId: child.rootNodeId,
      kind: "tool_call",
      status,
      countsForRun: false,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: state.startedAt,
      completedAt: base.completedAt,
    };
    yield* deps.emit({ type: "node.updated", driver: deps.driver, node });
    yield* deps.emit({ type: "turn_item.updated", driver: deps.driver, turnItem });
  });

  return {
    handles: (type: string) => CHILD_TOOL_EVENTS.has(type),

    /** Handles one of CHILD_TOOL_EVENTS that Copilot attributed to this child. */
    handle: Effect.fnUntraced(function* (
      child: ChildThreadRef,
      event: { readonly type: string; readonly data?: Record<string, unknown> | undefined },
    ) {
      const data = event.data ?? {};
      const toolCallId = text(data.toolCallId);
      if (toolCallId === undefined) return;
      const tools = toolsOf(child);
      if (event.type === "tool.execution_start") {
        const toolName = text(data.toolName) ?? "tool";
        if (NO_TOOL_ROW.has(toolName) || tools.has(toolCallId)) return;
        const state: ChildToolState = {
          nativeId: toolCallId,
          ordinal: child.nextOrdinal++,
          startedAt: yield* DateTime.now,
          toolName,
          toolTitle: text(data.toolTitle),
          args: record(data.arguments),
          text: "",
          status: "running",
          exitCode: undefined,
        };
        tools.set(toolCallId, state);
        yield* publish(child, state);
        return;
      }
      const state = tools.get(toolCallId);
      if (state === undefined) return;
      if (event.type === "tool.shell_output") {
        const chunk = text(data.text);
        if (chunk === undefined || chunk.length === 0) return;
        state.text += chunk;
        yield* deps.emit({
          type: "command_output.delta",
          driver: deps.driver,
          threadId: child.threadId,
          itemId: deps.turnItemId(identity(child, toolCallId)),
          chunk,
        });
        return;
      }
      // tool.execution_complete
      const result = record(data.result);
      const error = record(data.error);
      const exitCode = record(data.shellExecution).exitCode;
      state.text =
        text(result.detailedContent) ?? text(result.content) ?? text(error.message) ?? state.text;
      state.status = data.success === false ? "failed" : "completed";
      if (typeof exitCode === "number") state.exitCode = exitCode;
      yield* publish(child, state);
    }),

    /** Ends rows still running when their agent ends, so a stopped agent leaves none spinning. */
    close: Effect.fnUntraced(function* (
      child: ChildThreadRef,
      status: Extract<OrchestrationV2TurnItemStatus, "completed" | "failed" | "cancelled">,
    ) {
      for (const state of toolsOf(child).values()) {
        if (state.status !== "running") continue;
        state.status = status === "completed" ? "completed" : status;
        yield* publish(child, state);
      }
    }),
  };
}
