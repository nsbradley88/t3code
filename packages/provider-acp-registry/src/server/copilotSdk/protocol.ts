import type {
  OrchestrationV2TurnItem,
  OrchestrationV2TurnItemStatus,
  ProviderApprovalDecision,
  RuntimeMode,
} from "@t3tools/contracts";

/** The slice of Copilot's `api.schema.json` / `session-events` the adapter relies on. */

export type CopilotPermissionMode = "manual" | "assisted" | "allow-all";
export type CopilotSessionMode = "interactive" | "plan" | "autopilot";

/**
 * T3's full-access is Copilot's allow-all ("bypass permissions"). Copilot keeps permission mode
 * and agent mode (interactive/plan/autopilot) apart, so entering and leaving plan mode never
 * changes the permission mode by itself.
 */
export function copilotPermissionModeForRuntimeMode(mode: RuntimeMode): CopilotPermissionMode {
  return mode === "full-access" ? "allow-all" : "manual";
}

export interface CopilotPermissionRequest {
  readonly kind?: string;
  readonly intention?: string;
  readonly fullCommandText?: string;
  readonly commands?: ReadonlyArray<{ readonly identifier?: string }>;
  readonly canOfferSessionApproval?: boolean;
  readonly fileName?: string;
  readonly path?: string;
  readonly url?: string;
  readonly serverName?: string;
  readonly toolName?: string;
  readonly toolTitle?: string;
  readonly toolDescription?: string;
}

export function copilotPermissionKind(request: CopilotPermissionRequest) {
  switch (request.kind) {
    case "shell":
      return "command" as const;
    case "write":
      return "file-change" as const;
    case "read":
      return "file-read" as const;
    default:
      return "permission" as const;
  }
}

export function copilotPermissionPrompt(request: CopilotPermissionRequest): string {
  return (
    request.fullCommandText ??
    request.fileName ??
    request.path ??
    request.url ??
    request.toolTitle ??
    request.toolName ??
    request.intention ??
    "GitHub Copilot requests permission"
  );
}

function sessionApproval(request: CopilotPermissionRequest): Record<string, unknown> | undefined {
  switch (request.kind) {
    case "shell": {
      if (request.canOfferSessionApproval === false) return undefined;
      const identifiers = (request.commands ?? []).flatMap((command) =>
        command.identifier ? [command.identifier] : [],
      );
      return identifiers.length > 0
        ? { kind: "commands", commandIdentifiers: identifiers }
        : undefined;
    }
    case "read":
      return { kind: "read" };
    case "write":
      return request.canOfferSessionApproval === false ? undefined : { kind: "write" };
    case "mcp":
      return request.serverName
        ? { kind: "mcp", serverName: request.serverName, toolName: request.toolName ?? null }
        : undefined;
    case "custom-tool":
      return request.toolName ? { kind: "custom-tool", toolName: request.toolName } : undefined;
    default:
      return undefined;
  }
}

export function copilotApprovalOptions(request: CopilotPermissionRequest) {
  return [
    { decision: "accept" as const, label: "Allow once" },
    ...(sessionApproval(request)
      ? [{ decision: "acceptForSession" as const, label: "Allow for this session" }]
      : []),
    { decision: "decline" as const, label: "Deny" },
  ];
}

/** The `PermissionDecision` Copilot expects, or undefined when it did not offer that choice. */
export function copilotPermissionDecision(
  request: CopilotPermissionRequest,
  decision: ProviderApprovalDecision,
  feedback?: string,
): Record<string, unknown> | undefined {
  switch (decision) {
    case "accept":
      return { kind: "approve-once" };
    case "acceptForSession":
    case "acceptAlways": {
      const approval = sessionApproval(request);
      return approval ? { kind: "approve-for-session", approval } : undefined;
    }
    case "decline":
      return { kind: "reject", ...(feedback ? { feedback } : {}) };
    case "cancel":
      return { kind: "cancelled" };
  }
}

type Fields<
  T extends OrchestrationV2TurnItem["type"],
  K extends keyof Extract<OrchestrationV2TurnItem, { type: T }>,
> = Pick<Extract<OrchestrationV2TurnItem, { type: T }>, K>;

export type CopilotToolPresentation = {
  readonly status: OrchestrationV2TurnItemStatus;
  readonly title: string | null;
} & (
  | Fields<"command_execution", "type" | "input" | "output" | "exitCode">
  | Fields<"file_change", "type" | "fileName" | "oldStr" | "newStr" | "diffStr">
  | Fields<"web_search", "type" | "patterns" | "results">
  | Fields<"dynamic_tool", "type" | "toolName" | "input" | "output">
);

export function copilotToolPresentation(input: {
  readonly toolName: string;
  readonly title?: string | undefined;
  readonly args?: Record<string, unknown> | undefined;
  readonly output: string;
  readonly status: OrchestrationV2TurnItemStatus;
  readonly exitCode?: number | undefined;
}): CopilotToolPresentation {
  const { toolName, args, output, status } = input;
  const stringArg = (...keys: string[]) => {
    for (const key of keys) {
      const value = args?.[key];
      if (typeof value === "string") return value;
    }
    return undefined;
  };
  const shared = { status, title: input.title ?? toolName };
  if (/^(bash|shell|powershell|write_bash)$/i.test(toolName)) {
    return {
      ...shared,
      type: "command_execution",
      input: stringArg("command", "input") ?? "",
      output,
      ...(input.exitCode === undefined ? {} : { exitCode: input.exitCode }),
    };
  }
  if (/^(edit|create|str_replace\w*|apply_patch|write)$/i.test(toolName)) {
    const fileName = stringArg("path", "file_path", "filePath")?.trim();
    if (fileName) {
      const oldStr = stringArg("old_str", "old_string");
      const newStr = stringArg("new_str", "new_string", "file_text", "content");
      const diffStr = stringArg("diff", "patch");
      return {
        ...shared,
        type: "file_change",
        fileName,
        ...(oldStr === undefined ? {} : { oldStr }),
        ...(newStr === undefined ? {} : { newStr }),
        ...(diffStr === undefined ? {} : { diffStr }),
      };
    }
  }
  if (/^(web_fetch|web_search)$/i.test(toolName)) {
    const query = stringArg("query", "url")?.trim();
    const url = stringArg("url")?.trim();
    return {
      ...shared,
      type: "web_search",
      ...(query ? { patterns: [query] } : {}),
      ...(output ? { results: [{ ...(url ? { url } : {}), snippet: output }] } : {}),
    };
  }
  return { ...shared, type: "dynamic_tool", toolName, input: args ?? {}, output };
}
