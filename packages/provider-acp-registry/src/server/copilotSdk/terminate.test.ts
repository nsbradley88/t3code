// @effect-diagnostics nodeBuiltinImport:off -- A real stub CLI process proves which termination path runs.
import * as NodeProcess from "node:process";
import { assert, describe, it } from "@effect/vitest";

import { openCopilotSdkConnection, terminateCopilotProcess } from "./connection.ts";

const recorder = () => {
  const log: Array<string> = [];
  return {
    log,
    killChild: (signal: NodeJS.Signals) => void log.push(`child:${signal}`),
    killGroup: (pid: number, signal: NodeJS.Signals) => void log.push(`group:${pid}:${signal}`),
    runTaskkill: (pid: number, _fallback: () => void) => void log.push(`taskkill:${pid}`),
  };
};

describe("terminateCopilotProcess", () => {
  it("reaps the whole process tree with taskkill on Windows, never a process group", () => {
    const r = recorder();
    terminateCopilotProcess({ platform: "win32", pid: 4242, ...r });
    assert.deepStrictEqual(r.log, ["taskkill:4242"]);
  });

  it("falls back to killing the child when Windows has no pid or taskkill cannot run", () => {
    const noPid = recorder();
    terminateCopilotProcess({ platform: "win32", pid: undefined, ...noPid });
    assert.deepStrictEqual(noPid.log, ["child:SIGTERM"]);

    const throwing = recorder();
    terminateCopilotProcess({
      platform: "win32",
      pid: 7,
      ...throwing,
      runTaskkill: () => {
        throw new Error("spawn taskkill ENOENT");
      },
    });
    assert.deepStrictEqual(throwing.log, ["child:SIGTERM"]);

    // taskkill that starts but then fails reports back through the fallback it was given.
    const failing = recorder();
    terminateCopilotProcess({
      platform: "win32",
      pid: 7,
      ...failing,
      runTaskkill: (_pid, fallback) => fallback(),
    });
    assert.deepStrictEqual(failing.log, ["child:SIGTERM"]);
  });

  it("signals the process group elsewhere, and falls back to the child when that fails", () => {
    const r = recorder();
    terminateCopilotProcess({ platform: "linux", pid: 99, ...r });
    assert.deepStrictEqual(r.log, ["group:99:SIGTERM"]);

    const gone = recorder();
    terminateCopilotProcess({
      platform: "darwin",
      pid: 99,
      ...gone,
      killGroup: () => {
        throw Object.assign(new Error("kill ESRCH"), { code: "ESRCH" });
      },
    });
    assert.deepStrictEqual(gone.log, ["child:SIGTERM"]);
  });
});

describe("openCopilotSdkConnection shutdown", () => {
  const script =
    "process.stdin.on('data',()=>{const b=JSON.stringify({jsonrpc:'2.0',id:1,result:{message:'pong',protocolVersion:3}});" +
    "process.stdout.write('Content-Length: '+Buffer.byteLength(b)+'\\r\\n\\r\\n'+b)});setInterval(()=>{},1000)";
  const spawnInput = (platform: NodeJS.Platform, runTaskkill?: (pid: number) => void) => ({
    command: NodeProcess.execPath,
    args: ["-e", script, "--"],
    env: NodeProcess.env,
    cwd: NodeProcess.cwd(),
    platform,
    ...(runTaskkill === undefined ? {} : { runTaskkill }),
  });

  it("closes a Windows connection with taskkill on the CLI's own pid", async () => {
    const killed: Array<number> = [];
    const connection = await openCopilotSdkConnection(
      spawnInput("win32", (pid) => {
        killed.push(pid);
        // The test stands in for taskkill so the stub process really ends.
        process.kill(pid, "SIGTERM");
      }),
    );
    await connection.close();
    assert.strictEqual(connection.isClosed(), true);
    assert.strictEqual(killed.length, 1);
    assert.isTrue(Number.isInteger(killed[0]) && killed[0]! > 1);
  });

  it("closes a non-Windows connection without ever calling taskkill", async () => {
    let calls = 0;
    const connection = await openCopilotSdkConnection(
      spawnInput("linux", () => {
        calls += 1;
      }),
    );
    await connection.close();
    assert.strictEqual(connection.isClosed(), true);
    assert.strictEqual(calls, 0);
  });
});
