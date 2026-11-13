// @effect-diagnostics-next-line nodeBuiltinImport:off -- JSON-RPC over the Copilot CLI's stdio.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Stream types for the framed transport.
import type * as NodeStream from "node:stream";

/**
 * Minimal JSON-RPC 2.0 client for `copilot --headless --stdio` (Content-Length framing, the
 * same wire format the Copilot SDK speaks). The protocol surface is Copilot's `api.schema.json`;
 * this module only moves frames and correlates ids.
 */

const COPILOT_SDK_PROTOCOL_VERSION = 3;

// @effect-diagnostics-next-line extendsNativeError:off -- Plain-Promise transport shared with non-Effect callers.
export class CopilotSdkRpcError extends Error {
  readonly code: number;
  readonly data: unknown;
  constructor(message: string, code: number, data?: unknown) {
    super(message);
    this.name = "CopilotSdkRpcError";
    this.code = code;
    this.data = data;
  }
}

export interface CopilotSdkCloseInfo {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stderr: string;
}

export interface CopilotSdkConnection {
  readonly request: (
    method: string,
    params?: Record<string, unknown>,
    options?: { readonly timeoutMs?: number },
  ) => Promise<unknown>;
  /** Handles every server notification; Copilot multiplexes sessions inside `session.event`. */
  readonly onNotification: (handler: (method: string, params: unknown) => void) => void;
  /** Server-to-client requests (`userInput.request`, `exitPlanMode.request`, ...). */
  readonly onRequest: (method: string, handler: (params: unknown) => Promise<unknown>) => void;
  readonly onClose: (handler: (info: CopilotSdkCloseInfo) => void) => void;
  readonly close: () => Promise<void>;
  readonly isClosed: () => boolean;
}

export interface CopilotSdkSpawnInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly env: NodeJS.ProcessEnv;
  readonly cwd: string;
  readonly platform: NodeJS.Platform;
}

/** Arguments that turn a resolved `copilot` command into the SDK JSON-RPC server. */
export function copilotSdkServerArgs(args: ReadonlyArray<string>): ReadonlyArray<string> {
  return [...args.filter((arg) => arg !== "--acp"), "--headless", "--stdio", "--no-auto-update"];
}

const HEADER_END = Buffer.from("\r\n\r\n");
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_FRAME_BYTES = 256 * 1024 * 1024;

interface JsonRpcMessage {
  readonly id?: number | string | null;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly error?: { readonly code: number; readonly message: string; readonly data?: unknown };
}

/** Wraps a duplex pair of byte streams in a JSON-RPC connection. */
export function makeFramedConnection(input: {
  readonly readable: NodeStream.Readable;
  readonly writable: NodeStream.Writable;
  readonly kill: () => void;
  readonly stderrTail: () => string;
}): {
  readonly connection: CopilotSdkConnection;
  /** Called by the owner once the transport has ended. */
  readonly notifyClosed: (code: number | null, signal: NodeJS.Signals | null) => void;
} {
  let buffer: Buffer = Buffer.alloc(0);
  let nextId = 1;
  let closed = false;
  let notification: (method: string, params: unknown) => void = () => {};
  const closeHandlers: Array<(info: CopilotSdkCloseInfo) => void> = [];
  const requestHandlers = new Map<string, (params: unknown) => Promise<unknown>>();
  const pending = new Map<
    number | string,
    {
      readonly resolve: (value: unknown) => void;
      readonly reject: (reason: unknown) => void;
      readonly timer: ReturnType<typeof setTimeout>;
      readonly method: string;
    }
  >();

  const send = (message: Record<string, unknown>) => {
    const body = Buffer.from(JSON.stringify({ jsonrpc: "2.0", ...message }), "utf8");
    if (closed || input.writable.destroyed) return false;
    input.writable.write(
      Buffer.concat([Buffer.from(`Content-Length: ${body.length}\r\n\r\n`), body]),
    );
    return true;
  };

  const failAll = (reason: Error) => {
    for (const [id, entry] of pending) {
      clearTimeout(entry.timer);
      pending.delete(id);
      entry.reject(reason);
    }
  };

  const dispatch = (message: JsonRpcMessage) => {
    if (message.method !== undefined) {
      if (message.id === undefined || message.id === null) {
        notification(message.method, message.params);
        return;
      }
      const id = message.id;
      const handler = requestHandlers.get(message.method);
      if (!handler) {
        send({ id, error: { code: -32601, message: `Method not found: ${message.method}` } });
        return;
      }
      void handler(message.params).then(
        (result) => send({ id, result: result ?? null }),
        (cause: unknown) =>
          send({
            id,
            error: {
              code: -32603,
              message: cause instanceof Error ? cause.message : "Client request failed",
            },
          }),
      );
      return;
    }
    if (message.id === undefined || message.id === null) return;
    const entry = pending.get(message.id);
    if (!entry) return;
    pending.delete(message.id);
    clearTimeout(entry.timer);
    if (message.error) {
      entry.reject(
        new CopilotSdkRpcError(
          `${entry.method}: ${message.error.message}`,
          message.error.code,
          message.error.data,
        ),
      );
    } else entry.resolve(message.result);
  };

  const drain = () => {
    while (true) {
      const headerEnd = buffer.indexOf(HEADER_END);
      if (headerEnd < 0) return;
      const header = buffer.subarray(0, headerEnd).toString("ascii");
      const match = /content-length:\s*(\d+)/i.exec(header);
      if (!match) {
        // Not a framed stream; nothing downstream can recover.
        buffer = Buffer.alloc(0);
        shutdown(new Error("Copilot SDK server sent a frame without Content-Length"));
        return;
      }
      const length = Number(match[1]);
      if (length > MAX_FRAME_BYTES) {
        shutdown(new Error("Copilot SDK server sent an oversized frame"));
        return;
      }
      const bodyStart = headerEnd + HEADER_END.length;
      if (buffer.length < bodyStart + length) return;
      const body = buffer.subarray(bodyStart, bodyStart + length);
      buffer = buffer.subarray(bodyStart + length);
      let parsed: JsonRpcMessage;
      try {
        parsed = JSON.parse(body.toString("utf8")) as JsonRpcMessage;
      } catch {
        continue;
      }
      dispatch(parsed);
    }
  };

  const shutdown = (reason: Error) => {
    if (closed) return;
    closed = true;
    failAll(reason);
    input.kill();
  };

  input.readable.on("data", (chunk: Buffer) => {
    buffer = buffer.length === 0 ? chunk : Buffer.concat([buffer, chunk]);
    drain();
  });
  input.readable.on("error", (cause) =>
    shutdown(cause instanceof Error ? cause : new Error(String(cause))),
  );
  input.writable.on("error", () => {});

  const connection: CopilotSdkConnection = {
    request: (method, params = {}, options) =>
      new Promise<unknown>((resolve, reject) => {
        if (closed) {
          reject(new Error(`Copilot SDK server is closed (${method})`));
          return;
        }
        const id = nextId++;
        const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
        // @effect-diagnostics-next-line globalTimers:off -- Plain-Promise transport.
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Copilot SDK request ${method} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
        pending.set(id, { resolve, reject, timer, method });
        if (!send({ id, method, params })) {
          pending.delete(id);
          clearTimeout(timer);
          reject(new Error(`Copilot SDK server is closed (${method})`));
        }
      }),
    onNotification: (handler) => {
      notification = handler;
    },
    onRequest: (method, handler) => {
      requestHandlers.set(method, handler);
    },
    onClose: (handler) => {
      closeHandlers.push(handler);
    },
    close: async () => {
      if (closed) return;
      closed = true;
      failAll(new Error("Copilot SDK connection closed"));
      input.kill();
    },
    isClosed: () => closed,
  };

  return {
    connection,
    notifyClosed: (code, signal) => {
      const wasClosed = closed;
      closed = true;
      failAll(
        new Error(
          `Copilot SDK server exited${code === null ? "" : ` with code ${code}`}${
            input.stderrTail() ? `: ${input.stderrTail()}` : ""
          }`,
        ),
      );
      if (!wasClosed || closeHandlers.length > 0)
        for (const handler of closeHandlers) handler({ code, signal, stderr: input.stderrTail() });
    },
  };
}

/** Launches `copilot --headless --stdio` and verifies the protocol with `ping`. */
export async function openCopilotSdkConnection(
  input: CopilotSdkSpawnInput,
  options: { readonly timeoutMs?: number } = {},
): Promise<CopilotSdkConnection> {
  const child = NodeChildProcess.spawn(input.command, [...copilotSdkServerArgs(input.args)], {
    cwd: input.cwd,
    env: input.env,
    stdio: ["pipe", "pipe", "pipe"],
    // Own process group, so shutdown can reap shells and background agents too.
    detached: input.platform !== "win32",
  });
  let stderr = "";
  child.stderr.on("data", (chunk: Buffer) => {
    stderr = (stderr + chunk.toString("utf8")).slice(-4_000);
  });
  const killTree = () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    try {
      if (input.platform !== "win32" && child.pid !== undefined)
        process.kill(-child.pid, "SIGTERM");
      else child.kill("SIGTERM");
    } catch {
      child.kill("SIGTERM");
    }
  };
  const { connection, notifyClosed } = makeFramedConnection({
    readable: child.stdout,
    writable: child.stdin,
    kill: killTree,
    stderrTail: () => stderr.trim(),
  });
  child.on("exit", (code, signal) => notifyClosed(code, signal));
  const spawnFailed = new Promise<never>((_, reject) => {
    child.once("error", (cause) => {
      reject(new Error(`Cannot launch ${input.command}: ${cause.message}`));
    });
  });
  try {
    const pong = (await Promise.race([
      connection.request("ping", {}, { timeoutMs: options.timeoutMs ?? 20_000 }),
      spawnFailed,
    ])) as { readonly protocolVersion?: number } | undefined;
    if (pong?.protocolVersion === undefined || pong.protocolVersion < COPILOT_SDK_PROTOCOL_VERSION)
      throw new Error(
        `Copilot SDK server protocol ${pong?.protocolVersion ?? "unknown"} is older than ${COPILOT_SDK_PROTOCOL_VERSION}; update GitHub Copilot CLI`,
      );
  } catch (cause) {
    await connection.close();
    throw cause;
  }
  return connection;
}
