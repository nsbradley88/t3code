/**
 * Maps T3 provider turns onto Copilot's session journal for snapshot, rollback and fork. A turn
 * begins at a root `user.message` event, whose id is its strong native turn ref. Rewinds are
 * conversation-only: T3's checkpoint service restores files itself, so Copilot must not too.
 */
import type {
  OrchestrationV2ConversationMessage,
  OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

/** A user message can reach Copilot a little after T3 opens the turn that sent it. */
const TURN_START_SLACK_MS = 2_000;
const JOURNAL_PAGE_SIZE = 500;
const JOURNAL_MAX_PAGES = 400;

export interface JournalEvent {
  readonly id: string;
  readonly parentId: string | null;
  readonly type: string;
  readonly timestampMs: number | null;
  /** Present for events that belong to a subagent rather than the main agent. */
  readonly agentId: string | undefined;
  readonly content: string;
  readonly delivery: string | undefined;
}

const record = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const text = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;

export function parseJournalEvent(raw: unknown): JournalEvent | undefined {
  const event = record(raw);
  const id = text(event.id);
  const type = text(event.type);
  if (id === undefined || type === undefined) return undefined;
  const data = record(event.data);
  const timestamp = text(event.timestamp);
  const parsed = timestamp === undefined ? Number.NaN : Date.parse(timestamp);
  return {
    id,
    parentId: text(event.parentId) ?? null,
    type,
    timestampMs: Number.isNaN(parsed) ? null : parsed,
    agentId: text(event.agentId),
    content: text(data.content) ?? "",
    delivery: text(data.delivery),
  };
}

/**
 * A root user message that begins a turn. Steering lands inside the run already in flight, so it
 * is not a turn of its own, and a subagent's prompt belongs to the subagent.
 */
export function isTurnStart(event: {
  readonly type: string;
  readonly agentId: string | undefined;
  readonly delivery: string | undefined;
}): boolean {
  return (
    event.type === "user.message" && event.agentId === undefined && event.delivery !== "steering"
  );
}

/** The first discarded turn decides where history is cut, so only turns after the target count. */
export function discardedTurns(input: {
  readonly target:
    | { readonly type: "thread_start" }
    | { readonly type: "provider_turn"; readonly providerTurn: OrchestrationV2ProviderTurn };
  readonly providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
}): ReadonlyArray<OrchestrationV2ProviderTurn> {
  const after = input.target.type === "thread_start" ? 0 : input.target.providerTurn.ordinal;
  return input.providerThreadTurns
    .filter((turn) => turn.ordinal > after)
    .toSorted((left, right) => left.ordinal - right.ordinal);
}

export interface BoundaryCandidate {
  readonly id: string;
  readonly timestampMs: number | null;
}

/**
 * The event that began the first discarded turn that Copilot's history can cut at. A strong ref
 * names it directly. Turns T3 could not tie to an event (a wake from background agents, a
 * compaction) are matched by when they ran, and a turn with no matching event leaves nothing to
 * cut, so the next discarded turn decides.
 */
export function resolveBoundaryEvent(
  candidates: ReadonlyArray<BoundaryCandidate>,
  turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): string | undefined {
  const known = new Set(candidates.map((candidate) => candidate.id));
  const byTime = candidates
    .filter((candidate) => candidate.timestampMs !== null)
    .toSorted((left, right) => left.timestampMs! - right.timestampMs!);
  for (const turn of turns) {
    const ref = turn.nativeTurnRef;
    if (ref?.strength === "strong" && ref.nativeId !== null && known.has(ref.nativeId)) {
      return ref.nativeId;
    }
    if (turn.startedAt === null) continue;
    const from = DateTime.toEpochMillis(turn.startedAt) - TURN_START_SLACK_MS;
    const to =
      turn.completedAt === null
        ? Number.POSITIVE_INFINITY
        : DateTime.toEpochMillis(turn.completedAt) + TURN_START_SLACK_MS;
    const matched = byTime.find(
      (candidate) => candidate.timestampMs! >= from && candidate.timestampMs! <= to,
    );
    if (matched !== undefined) return matched.id;
  }
  return undefined;
}

/**
 * Copilot forks "through" an event, so a fork that ends before a turn cuts at the event right
 * before that turn's message. Null when the turn has no parent to cut at.
 */
export function forkCutoffBefore(
  events: ReadonlyArray<JournalEvent>,
  boundaryEventId: string,
): string | null {
  return events.find((event) => event.id === boundaryEventId)?.parentId ?? null;
}

export interface JournalRead {
  readonly events: ReadonlyArray<JournalEvent>;
  /** True when the journal outran the page budget or its cursor expired mid-read. */
  readonly truncated: boolean;
}

/** Reads a session's persisted journal without activating the session. */
export const readJournal = Effect.fn("CopilotJournal.read")(function* <E>(
  request: (method: string, params: Record<string, unknown>) => Effect.Effect<unknown, E>,
  sessionId: string,
) {
  const events: Array<JournalEvent> = [];
  let cursor: string | undefined;
  for (let page = 0; page < JOURNAL_MAX_PAGES; page += 1) {
    const result = record(
      yield* request("sessions.readPersistedEvents", {
        sessionId,
        max: JOURNAL_PAGE_SIZE,
        direction: "forward",
        ...(cursor === undefined ? {} : { cursor }),
      }),
    );
    if (text(result.cursorStatus) === "expired") return { events, truncated: true };
    const batch = Array.isArray(result.events) ? result.events : [];
    for (const raw of batch) {
      const parsed = parseJournalEvent(raw);
      if (parsed !== undefined) events.push(parsed);
    }
    cursor = text(result.cursor);
    if (result.hasMore !== true || cursor === undefined) return { events, truncated: false };
  }
  return { events, truncated: true } satisfies JournalRead;
});

/** The conversation a thread snapshot shows: main-agent user and assistant messages, in order. */
export function journalMessages(input: {
  readonly events: ReadonlyArray<JournalEvent>;
  readonly threadId: OrchestrationV2ConversationMessage["threadId"];
  readonly fallbackTime: DateTime.Utc;
  readonly messageId: (eventId: string) => OrchestrationV2ConversationMessage["id"];
}): ReadonlyArray<OrchestrationV2ConversationMessage> {
  return input.events.flatMap((event) => {
    if (event.agentId !== undefined) return [];
    const role =
      event.type === "user.message"
        ? "user"
        : event.type === "assistant.message"
          ? "assistant"
          : undefined;
    if (role === undefined || event.content.trim().length === 0) return [];
    const at = Option.getOrElse(
      DateTime.make(event.timestampMs ?? Number.NaN),
      () => input.fallbackTime,
    );
    return [
      {
        id: input.messageId(event.id),
        threadId: input.threadId,
        runId: null,
        nodeId: null,
        role,
        text: event.content,
        attachments: [],
        streaming: false,
        createdBy: role === "user" ? ("user" as const) : ("agent" as const),
        creationSource: "provider" as const,
        createdAt: at,
        updatedAt: at,
      },
    ];
  });
}
