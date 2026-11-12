import type { RuntimeMode } from "@t3tools/contracts";
import type { AcpToolCallState } from "@t3tools/provider-acp/server/runtimeModel";

const COPILOT_MODE = "https://agentclientprotocol.com/protocol/session-modes#";

// Copilot has no approval classifier, and ACP client policy would answer `auto` as full access.
export const COPILOT_SUPPORTED_RUNTIME_MODES = [
  "approval-required",
  "auto-accept-edits",
  "full-access",
] as const satisfies ReadonlyArray<RuntimeMode>;

/** Agent and Plan ask T3 before gated tools; Autopilot turns on Copilot's own allow-all. */
export function copilotRuntimeModeForSessionMode(modeId: string): RuntimeMode | undefined {
  switch (modeId) {
    case `${COPILOT_MODE}agent`:
    case `${COPILOT_MODE}plan`:
      return "approval-required";
    case `${COPILOT_MODE}autopilot`:
      return "full-access";
    default:
      return undefined;
  }
}

const COPILOT_PLAN_FILE = /^(?:\/|[A-Za-z]:[\\/]).*[\\/]session-state[\\/][^\\/]+[\\/]plan\.md$/;

/**
 * Over ACP, Copilot 1.0.109 offers no exit_plan_mode tool: plan mode writes the plan to the
 * session's `plan.md` with `create` or patches it with `edit`, whose diff is only the hunk.
 */
export function copilotProposedPlanFilePath(tool: AcpToolCallState): string | undefined {
  const input = tool.data.rawInput;
  const path =
    typeof input === "object" && input !== null ? (input as { path?: unknown }).path : undefined;
  return typeof path === "string" && COPILOT_PLAN_FILE.test(path) ? path : undefined;
}
