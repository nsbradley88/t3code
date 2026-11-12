// @effect-diagnostics nodeBuiltinImport:off -- In-memory pipes and a stub child process stand in for the CLI.
import * as NodeProcess from "node:process";
import * as NodeStream from "node:stream";
import { assert, describe, it } from "@effect/vitest";

import {
  CopilotSdkRpcError,
  copilotSdkServerArgs,
  makeFramedConnection,
  openCopilotSdkConnection,
} from "./connection.ts";

const frame = (message: unknown) => {
  const body = Buffer.from(JSON.stringify({ jsonrpc: "2.0", ...(message as object) }));
  return Buffer.concat([Buffer.from(`Content-Length: ${body.length}\r\n\r\n`), body]);
};

function harness() {
  const serverToClient = new NodeStream.PassThrough();
  const clientToServer = new NodeStream.PassThrough();
  const sent: Array<Record<string, unknown>> = [];
  const waiters: Array<{ count: number; resolve: () => void }> = [];
  let pending = Buffer.alloc(0);
  clientToServer.on("data", (chunk: Buffer) => {
    pending = Buffer.concat([pending, chunk]);
    while (true) {
      const end = pending.indexOf("\r\n\r\n");
      if (end < 0) return;
      const length = Number(/Content-Length: (\d+)/i.exec(pending.subarray(0, end).toString())![1]);
      if (pending.length < end + 4 + length) return;
      sent.push(JSON.parse(pending.subarray(end + 4, end + 4 + length).toString()));
      pending = pending.subarray(end + 4 + length);
      for (const waiter of waiters.filter((candidate) => sent.length >= candidate.count))
        waiter.resolve();
    }
  });
  let killed = 0;
  const { connection, notifyClosed } = makeFramedConnection({
    readable: serverToClient,
    writable: clientToServer,
    kill: () => void (killed += 1),
    stderrTail: () => "",
  });
  // Resolves once the client has written `count` frames.
  const sentAtLeast = (count: number) =>
    sent.length >= count
      ? Promise.resolve()
      : new Promise<void>((resolve) => waiters.push({ count, resolve }));
  return { connection, notifyClosed, serverToClient, sent, sentAtLeast, killed: () => killed };
}

describe("Copilot SDK connection", () => {
  it("adds the headless stdio flags and drops --acp", () => {
    assert.deepStrictEqual(copilotSdkServerArgs(["--acp"]), [
      "--headless",
      "--stdio",
      "--no-auto-update",
    ]);
  });

  it("correlates responses to requests, including frames split across chunks", async () => {
    const h = harness();
    const first = h.connection.request("ping", {});
    const second = h.connection.request("session.send", { prompt: "hi" });
    await h.sentAtLeast(2);
    assert.deepStrictEqual(
      h.sent.map((message) => message.method),
      ["ping", "session.send"],
    );
    const reply = Buffer.concat([
      frame({ id: h.sent[1]!.id, result: { messageId: "b" } }),
      frame({ id: h.sent[0]!.id, result: { message: "pong" } }),
    ]);
    // Split mid-header and mid-body.
    h.serverToClient.write(reply.subarray(0, 9));
    h.serverToClient.write(reply.subarray(9, 40));
    h.serverToClient.write(reply.subarray(40));
    assert.deepStrictEqual(await second, { messageId: "b" });
    assert.deepStrictEqual(await first, { message: "pong" });
  });

  it("surfaces JSON-RPC errors with their code", async () => {
    const h = harness();
    const call = h.connection.request("session.resume", {});
    await h.sentAtLeast(1);
    h.serverToClient.write(
      frame({ id: h.sent[0]!.id, error: { code: -32000, message: "Session not found" } }),
    );
    const error = await call.catch((cause: unknown) => cause);
    assert.ok(error instanceof CopilotSdkRpcError);
    assert.strictEqual(error.code, -32000);
  });

  it("delivers notifications and answers registered server requests", async () => {
    const h = harness();
    const seen: Array<[string, unknown]> = [];
    h.connection.onNotification((method, params) => seen.push([method, params]));
    h.connection.onRequest("userInput.request", async (params) => ({
      answer: (params as { question: string }).question.toUpperCase(),
    }));
    h.serverToClient.write(frame({ method: "session.event", params: { sessionId: "s" } }));
    h.serverToClient.write(
      frame({ id: 7, method: "userInput.request", params: { question: "ok?" } }),
    );
    await h.sentAtLeast(1);
    assert.deepStrictEqual(seen, [["session.event", { sessionId: "s" }]]);
    assert.deepStrictEqual(h.sent.at(-1), { jsonrpc: "2.0", id: 7, result: { answer: "OK?" } });
  });

  it("rejects server requests it does not handle instead of hanging them", async () => {
    const h = harness();
    h.serverToClient.write(frame({ id: 9, method: "hooks.invoke", params: {} }));
    await h.sentAtLeast(1);
    const reply = h.sent.at(-1) as { id: number; error: { code: number } };
    assert.strictEqual(reply.id, 9);
    assert.strictEqual(reply.error.code, -32601);
  });

  it("fails pending requests when the server exits", async () => {
    const h = harness();
    const closes: Array<number | null> = [];
    h.connection.onClose((info) => closes.push(info.code));
    const call = h.connection.request("session.send", {});
    await h.sentAtLeast(1);
    h.notifyClosed(1, null);
    const error = await call.catch((cause: unknown) => cause);
    assert.match(String(error), /exited with code 1/);
    assert.deepStrictEqual(closes, [1]);
    assert.strictEqual(h.connection.isClosed(), true);
  });

  it("times out a request the server never answers", async () => {
    const h = harness();
    const error = await h.connection
      .request("session.send", {}, { timeoutMs: 20 })
      .catch((cause: unknown) => cause);
    assert.match(String(error), /timed out after 20ms/);
  });

  it("refuses a server older than protocol 3", async () => {
    const script =
      "const b=JSON.stringify({jsonrpc:'2.0',id:1,result:{message:'pong',protocolVersion:2}});" +
      "process.stdin.once('data',()=>process.stdout.write('Content-Length: '+Buffer.byteLength(b)+'\\r\\n\\r\\n'+b));setInterval(()=>{},1000)";
    const error = await openCopilotSdkConnection({
      command: NodeProcess.execPath,
      args: ["-e", script, "--"],
      env: NodeProcess.env,
      cwd: NodeProcess.cwd(),
      platform: "linux",
    }).catch((cause: unknown) => cause);
    assert.match(String(error), /protocol 2 is older than 3/);
  });

  it("opens against a conforming server and closes its process", async () => {
    const script =
      "process.stdin.on('data',()=>{const b=JSON.stringify({jsonrpc:'2.0',id:1,result:{message:'pong',protocolVersion:3}});" +
      "process.stdout.write('Content-Length: '+Buffer.byteLength(b)+'\\r\\n\\r\\n'+b)});setInterval(()=>{},1000)";
    const connection = await openCopilotSdkConnection({
      command: NodeProcess.execPath,
      args: ["-e", script, "--"],
      env: NodeProcess.env,
      cwd: NodeProcess.cwd(),
      platform: "linux",
    });
    assert.strictEqual(connection.isClosed(), false);
    await connection.close();
    assert.strictEqual(connection.isClosed(), true);
  });
});
