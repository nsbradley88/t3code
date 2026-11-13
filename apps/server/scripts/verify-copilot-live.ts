// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off globalFetch:off globalConsole:off globalJsonInEffect:off - Host-side process verifier retains raw wire evidence and owns deadline timers.
/**
 * Drives real Copilot threads through an isolated T3 server over the apps' WebSocket RPC.
 * Usage: node apps/server/scripts/verify-copilot-live.ts [--model <id>] [--scenario all|steer,...]
 */
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Socket } from "effect/socket";
import { RpcClient, RpcSerialization } from "effect/rpc";
import {
  ORCHESTRATION_PROTOCOL_VERSION,
  ORCHESTRATION_PROTOCOL_HEADER,
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  WsRpcGroup,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";

const { values } = NodeUtil.parseArgs({
  options: {
    model: { type: "string", default: "default" },
    scenario: { type: "string", default: "all" },
    timeout: { type: "string", default: "420" },
  },
});
const SCENARIOS = ["catalog", "steer", "interrupt", "modes", "usage", "child", "subagent"];
const selected = values.scenario === "all" ? SCENARIOS : values.scenario.split(",");
for (const name of selected) NodeAssert.ok(SCENARIOS.includes(name), `unknown scenario ${name}`);
const timeoutMs = Number(values.timeout) * 1000;
const root = NodePath.resolve(import.meta.dirname, "../../..");
const evidence = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-copilot-live-"));
const home = NodePath.join(evidence, "home");
const project = NodePath.join(evidence, "project");
NodeFS.mkdirSync(project);
NodeChildProcess.execFileSync("git", ["init", "-q", project]);
NodeChildProcess.execFileSync("git", [
  "-C",
  project,
  "-c",
  "user.name=Verifier",
  "-c",
  "user.email=verifier@localhost",
  "commit",
  "--allow-empty",
  "-qm",
  "initial",
]);
console.log(`Evidence: ${evidence}`);

const sleep = (ms: number) => new Promise<void>((done) => setTimeout(done, ms));
const log = (line: string) => {
  const text = `${new Date().toISOString()} ${line}`;
  console.log(text);
  NodeFS.appendFileSync(NodePath.join(evidence, "run.log"), `${text}\n`);
};

const reservation = NodeHttp.createServer();
reservation.listen(0, "127.0.0.1");
await new Promise<void>((done) => reservation.once("listening", done));
const address = reservation.address();
NodeAssert.ok(address && typeof address !== "string");
await new Promise<void>((done) => reservation.close(() => done()));
const port = address.port;
const origin = `http://127.0.0.1:${port}`;
const bootstrap = NodeCrypto.randomUUID();

const server = NodeChildProcess.spawn(
  process.execPath,
  ["apps/server/src/bin.ts", "start", "--bootstrap-fd", "3", "--log-level", "debug"],
  { cwd: root, detached: true, stdio: ["ignore", "pipe", "pipe", "pipe"] },
);
const bootstrapPipe = server.stdio[3];
NodeAssert.ok(bootstrapPipe && "write" in bootstrapPipe);
bootstrapPipe.end(
  JSON.stringify({
    mode: "desktop",
    noBrowser: true,
    port,
    t3Home: home,
    host: "127.0.0.1",
    desktopBootstrapToken: bootstrap,
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
  }),
);
const ready = Promise.withResolvers<void>();
let tail = "";
for (const pipe of [server.stdout, server.stderr])
  pipe?.on("data", (data: Buffer) => {
    const text = data.toString().replaceAll(bootstrap, "[REDACTED]");
    NodeFS.appendFileSync(NodePath.join(evidence, "server.log"), text);
    tail = (tail + text).slice(-8192);
    if (tail.includes("startup phase: complete")) ready.resolve();
  });
server.once("error", ready.reject);
server.once("exit", (code) => ready.reject(new Error(`Server exited ${code}`)));
const startupDeadline = setTimeout(
  () => ready.reject(new Error("Server startup timed out")),
  90_000,
);

type Verdict = { status: "passed" | "failed"; detail: unknown };
const verdicts: Record<string, Verdict> = {};
const abort = new AbortController();
const onSignal = () => abort.abort(new Error("Verification interrupted"));
process.on("SIGINT", onSignal);
process.on("SIGTERM", onSignal);

const isTerminalRun = (status: string) =>
  ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(status);

try {
  await ready.promise.finally(() => clearTimeout(startupDeadline));
  const tokenResponse = await fetch(`${origin}/oauth/token`, {
    method: "POST",
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: bootstrap,
      subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
    }),
  });
  NodeAssert.ok(tokenResponse.ok, `Authentication failed: ${tokenResponse.status}`);
  const token = Schema.decodeUnknownSync(Schema.Struct({ access_token: Schema.String }))(
    await tokenResponse.json(),
  );
  const headers = {
    Authorization: `Bearer ${token.access_token}`,
    "Content-Type": "application/json",
    [ORCHESTRATION_PROTOCOL_HEADER]: String(ORCHESTRATION_PROTOCOL_VERSION),
  };
  const projectId = ProjectId.make(NodeCrypto.randomUUID());
  const created = await fetch(`${origin}/api/projects/mutate`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId,
      title: "Copilot live verification",
      workspaceRoot: project,
    }),
  });
  NodeAssert.ok(created.ok, `Project creation failed: ${created.status} ${await created.text()}`);
  const ticketResponse = await fetch(`${origin}/api/auth/websocket-ticket`, {
    method: "POST",
    headers,
  });
  NodeAssert.ok(ticketResponse.ok, `WebSocket authentication failed: ${ticketResponse.status}`);
  const ticket = Schema.decodeUnknownSync(Schema.Struct({ ticket: Schema.String }))(
    await ticketResponse.json(),
  );
  const layerSocket = Socket.layerWebSocket(
    `${origin.replace("http:", "ws:")}/ws?orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}&wsTicket=${encodeURIComponent(ticket.ticket)}`,
  ).pipe(Layer.provide(Socket.layerWebSocketConstructorGlobal));
  const layerProtocol = RpcClient.layerProtocolSocket().pipe(
    Layer.provide(layerSocket),
    Layer.provide(RpcSerialization.layerJson),
  );
  const instanceId = ProviderInstanceId.make("copilot");

  const program = Effect.gen(function* () {
    const client = yield* RpcClient.make(WsRpcGroup);
    const run = <A>(effect: Effect.Effect<A, unknown>) => Effect.runPromise(effect);
    const dispatch = (command: Record<string, unknown>) =>
      run(
        client["orchestration.dispatchCommand"]({
          commandId: CommandId.make(NodeCrypto.randomUUID()),
          ...command,
        } as never) as Effect.Effect<unknown, unknown>,
      );
    const projectionOf = (threadId: ThreadId): Promise<OrchestrationV2ThreadProjection> =>
      run(
        client["orchestration.getThreadProjection"]({ threadId }) as Effect.Effect<
          OrchestrationV2ThreadProjection,
          unknown
        >,
      );
    const waitFor = async <A>(
      what: string,
      threadId: ThreadId,
      probe: (projection: OrchestrationV2ThreadProjection) => A | undefined | false,
      limitMs = 120_000,
    ): Promise<{ readonly value: A; readonly at: number }> => {
      const deadline = Date.now() + limitMs;
      for (;;) {
        const projection = await projectionOf(threadId);
        const found = probe(projection);
        if (found !== undefined && found !== false) return { value: found, at: Date.now() };
        if (Date.now() > deadline) {
          NodeFS.writeFileSync(
            NodePath.join(evidence, `timeout-${what.replace(/\W+/g, "-")}.json`),
            JSON.stringify(projection, null, 2),
          );
          throw new Error(`Timed out waiting for ${what}`);
        }
        await sleep(150);
      }
    };
    const saveProjection = async (name: string, threadId: ThreadId) =>
      NodeFS.writeFileSync(
        NodePath.join(evidence, `${name}.json`),
        JSON.stringify(await projectionOf(threadId), null, 2),
      );
    const launch = async (title: string, text: string, extra: Record<string, unknown> = {}) => {
      const threadId = ThreadId.make(NodeCrypto.randomUUID());
      await run(
        client["orchestration.launchThread"]({
          commandId: CommandId.make(NodeCrypto.randomUUID()),
          threadId,
          projectId,
          title,
          modelSelection: { instanceId, model: values.model },
          runtimeMode: "full-access",
          interactionMode: "default",
          workspaceStrategy: { type: "root" },
          initialMessage: { text, attachments: [] },
          ...extra,
        } as never) as Effect.Effect<unknown, unknown>,
      );
      return threadId;
    };
    const send = async (
      threadId: ThreadId,
      text: string,
      dispatchMode: Record<string, unknown>,
      extra: Record<string, unknown> = {},
    ) => {
      const messageId = MessageId.make(NodeCrypto.randomUUID());
      await dispatch({
        type: "message.dispatch",
        threadId,
        messageId,
        text,
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode,
        ...extra,
      });
      return messageId;
    };
    const activeRun = (projection: OrchestrationV2ThreadProjection) =>
      projection.runs.findLast((candidate) => !isTerminalRun(candidate.status));
    const runningTool = (projection: OrchestrationV2ThreadProjection) =>
      projection.turnItems.find(
        (item) => item.type !== "assistant_message" && item.status === "running",
      );
    const assistantText = (projection: OrchestrationV2ThreadProjection) =>
      projection.messages
        .filter((message) => message.role === "assistant")
        .map((message) => message.text)
        .join("\n");

    const scenario = async (name: string, body: () => Promise<unknown>) => {
      if (!selected.includes(name)) return;
      log(`--- scenario ${name}`);
      try {
        const detail = await body();
        verdicts[name] = { status: "passed", detail };
        log(`PASS ${name} ${JSON.stringify(detail)}`);
      } catch (error) {
        verdicts[name] = { status: "failed", detail: String(error) };
        log(`FAIL ${name} ${String(error)}`);
      }
    };

    // Enable Copilot in this isolated server and wait for its provider snapshot.
    yield* client["server.updateSettings"]({
      patch: {},
      providerInstanceMutation: {
        operation: "upsert",
        instanceId,
        instance: { driver: "copilot", enabled: true },
      },
    } as never) as Effect.Effect<unknown, unknown>;
    const copilotProvider = async () => {
      const deadline = Date.now() + 120_000;
      for (;;) {
        const config = (await run(
          client["server.getConfig"]({} as never) as Effect.Effect<unknown, unknown>,
        )) as { providers: ReadonlyArray<Record<string, any>> };
        const provider = config.providers.find((candidate) => candidate.instanceId === instanceId);
        if (
          provider &&
          provider.status === "ready" &&
          (provider.models?.length ?? 0) > 1 &&
          !String(provider.message ?? "").startsWith("Checking")
        )
          return provider;
        if (Date.now() > deadline)
          throw new Error(`Copilot provider never became ready: ${JSON.stringify(provider)}`);
        await sleep(1000);
      }
    };

    yield* Effect.promise(async () => {
      await copilotProvider();
      await scenario("catalog", async () => {
        const provider = await copilotProvider();
        NodeFS.writeFileSync(
          NodePath.join(evidence, "provider-snapshot.json"),
          JSON.stringify(provider, null, 2),
        );
        const optionIds = (model: Record<string, any>) =>
          (model.capabilities?.optionDescriptors ?? []).map(
            (option: Record<string, any>) => `${option.id}[${(option.options ?? []).length}]`,
          );
        const summary = {
          status: provider.status,
          version: provider.version,
          models: provider.models.length,
          defaultModel: provider.models.find((model: Record<string, any>) => model.isDefault)?.slug,
          firstModels: provider.models.slice(0, 4).map((model: Record<string, any>) => ({
            slug: model.slug,
            options: optionIds(model),
          })),
          slashCommands: (provider.slashCommands ?? []).map(
            (command: Record<string, any>) => command.name,
          ),
          skills: (provider.skills ?? []).length,
        };
        NodeAssert.ok(summary.models > 0, "no models published");
        NodeAssert.ok(summary.slashCommands.length > 0, "no slash commands published");
        return summary;
      });

      await scenario("steer", async () => {
        const threadId = await launch(
          "Copilot steer",
          "Run these 6 shell commands ONE AT A TIME, each as a separate tool call, never in parallel: `sleep 8; echo a`, then b, c, d, e, f (sleep 8 before each echo). Do nothing else.",
        );
        await waitFor(
          "a running tool",
          threadId,
          (projection) => runningTool(projection) && true,
          180_000,
        );
        await sleep(2000);
        const before = await projectionOf(threadId);
        const target = activeRun(before);
        NodeAssert.ok(target, "turn ended before the steer");
        const sentAt = Date.now();
        await send(
          threadId,
          "STOP the sleep loop now. Reply with exactly the single word PIVOT and nothing else.",
          { type: "steer_active", targetRunId: target.id },
          { deliveryIntent: "steer" },
        );
        const seen = await waitFor(
          "PIVOT reply",
          threadId,
          (projection) => /PIVOT/.test(assistantText(projection)) && true,
          60_000,
        );
        const latencyMs = seen.at - sentAt;
        await waitFor("steered run to settle", threadId, (projection) =>
          projection.runs.every((candidate) => isTerminalRun(candidate.status)),
        );
        const after = await projectionOf(threadId);
        await saveProjection("steer-projection", threadId);
        NodeAssert.ok(latencyMs < 15_000, `steer took ${latencyMs}ms`);
        return {
          latencyMs,
          runs: after.runs.map((candidate) => candidate.status),
          queuedRuns: after.runs.length - 1,
        };
      });

      await scenario("interrupt", async () => {
        const threadId = await launch(
          "Copilot interrupt",
          "Run `sleep 120; echo done` in the shell and wait for it. Do nothing else.",
        );
        await waitFor(
          "a running tool",
          threadId,
          (projection) => runningTool(projection) && true,
          180_000,
        );
        await sleep(1500);
        const target = activeRun(await projectionOf(threadId));
        NodeAssert.ok(target, "turn ended before the interrupt");
        const sentAt = Date.now();
        await dispatch({ type: "run.interrupt", threadId, runId: target.id });
        const settled = await waitFor(
          "interrupted run",
          threadId,
          (projection) =>
            projection.runs.find(
              (candidate) => candidate.id === target.id && isTerminalRun(candidate.status),
            ),
          60_000,
        );
        await saveProjection("interrupt-projection", threadId);
        return { settleMs: settled.at - sentAt, status: settled.value.status };
      });

      await scenario("modes", async () => {
        const threadId = await launch("Copilot modes", "Reply with exactly the word READY.", {
          interactionMode: "plan",
        });
        await waitFor(
          "plan turn",
          threadId,
          (projection) =>
            projection.runs.length > 0 &&
            projection.runs.every((candidate) => isTerminalRun(candidate.status)),
        );
        const sessionAfterPlan = (await projectionOf(threadId)).providerSessions.at(-1)
          ?.capabilities.sessions.supportsRuntimeModeSwitchInSession;
        // Back to default interaction in full-access: a shell write must run with no approval.
        await dispatch({
          type: "thread.interaction-mode.set",
          threadId,
          interactionMode: "default",
        });
        await dispatch({ type: "thread.runtime-mode.set", threadId, runtimeMode: "full-access" });
        const target = NodePath.join(project, "bypass-proof.txt");
        await send(
          threadId,
          `Run this exact shell command and nothing else: printf bypass > ${target}`,
          { type: "start_immediately" },
        );
        await waitFor(
          "bypass turn",
          threadId,
          (projection) =>
            projection.runs.length >= 2 &&
            projection.runs.every((candidate) => isTerminalRun(candidate.status)),
        );
        const projection = await projectionOf(threadId);
        await saveProjection("modes-projection", threadId);
        NodeAssert.ok(
          NodeFS.existsSync(target),
          "the shell write did not run (approval blocked it?)",
        );
        NodeAssert.equal(
          projection.runtimeRequests.filter((request) => request.kind === "command_approval")
            .length,
          0,
          "bypass still asked for approval",
        );
        return {
          switchableMidSession: sessionAfterPlan,
          runs: projection.runs.map((candidate) => candidate.status),
          approvals: projection.runtimeRequests.length,
        };
      });

      await scenario("usage", async () => {
        const threadId = await launch("Copilot usage", "Reply with exactly the word ONE.");
        await waitFor(
          "first turn",
          threadId,
          (projection) =>
            projection.runs.every((candidate) => isTerminalRun(candidate.status)) &&
            projection.runs.length === 1,
        );
        await send(threadId, "Reply with exactly the word TWO.", { type: "start_immediately" });
        await waitFor(
          "second turn",
          threadId,
          (projection) =>
            projection.runs.length === 2 &&
            projection.runs.every((candidate) => isTerminalRun(candidate.status)),
        );
        const projection = await projectionOf(threadId);
        await saveProjection("usage-projection", threadId);
        const usage = projection.providerTurns.map((turn) => ({
          ordinal: turn.ordinal,
          usage:
            (turn as Record<string, any>).tokenUsage ??
            (turn as Record<string, any>).turnTokenUsage ??
            null,
        }));
        NodeAssert.equal(usage.length >= 2, true, "expected two provider turns");
        NodeAssert.ok(
          usage.every((entry) => entry.usage !== null),
          "a turn reported no usage",
        );
        return usage;
      });

      await scenario("subagent", async () => {
        const threadId = await launch(
          "Copilot subagent",
          "Use your task tool to start ONE background general-purpose agent whose only job is to run `sleep 300` in the shell, then end your turn right away by saying STARTED. Do not wait for it.",
        );
        const started = await waitFor(
          "a running subagent",
          threadId,
          (projection) => projection.subagents.find((agent) => agent.status === "running"),
          240_000,
        );
        const projection = await projectionOf(threadId);
        const target = projection.runs.at(-1);
        NodeAssert.ok(target, "no run");
        const childId = projection.subagents.find(
          (agent) => agent.id === started.value.id,
        )?.childThreadId;
        NodeAssert.ok(childId, "the subagent has no child thread");
        const childPrompt = await waitFor("the child's prompt", ThreadId.make(childId), (child) =>
          child.messages.find((message) => message.role === "user"),
        );
        const sentAt = Date.now();
        await dispatch({
          type: "subagent.stop",
          threadId,
          runId: target.id,
          subagentId: started.value.id,
        });
        const stopped = await waitFor(
          "the subagent to stop",
          threadId,
          (current) =>
            current.subagents.find(
              (agent) => agent.id === started.value.id && agent.status !== "running",
            ),
          60_000,
        );
        await saveProjection("subagent-projection", threadId);
        return {
          stopMs: stopped.at - sentAt,
          status: stopped.value.status,
          childThreadId: stopped.value.childThreadId,
          childPrompt: childPrompt.value.text.slice(0, 80),
        };
      });

      await scenario("child", async () => {
        const threadId = await launch(
          "Copilot child",
          "Use your task tool to start ONE general-purpose agent whose job is: run `echo hello-from-agent` in the shell, then reply with the single word DONE. Wait for it to finish, then tell me what it said.",
        );
        const done = await waitFor(
          "a completed subagent",
          threadId,
          (projection) => projection.subagents.find((agent) => agent.status === "completed"),
          240_000,
        );
        const childId = done.value.childThreadId;
        NodeAssert.ok(childId, "the subagent has no child thread");
        const child = await projectionOf(ThreadId.make(childId));
        await waitFor("the parent run to settle", threadId, (projection) =>
          projection.runs.every((candidate) => isTerminalRun(candidate.status)),
        );
        NodeFS.writeFileSync(
          NodePath.join(evidence, "child-projection.json"),
          JSON.stringify(child, null, 2),
        );
        const roles = child.messages.map(
          (message) => `${message.role}: ${message.text.slice(0, 60)}`,
        );
        NodeAssert.ok(
          child.messages.some((message) => message.role === "user"),
          "no prompt in the child thread",
        );
        NodeAssert.ok(
          child.messages.some(
            (message) => message.role === "assistant" && /DONE/.test(message.text),
          ),
          "the child thread has no reply",
        );
        NodeAssert.ok(/DONE/.test(done.value.result ?? ""), "the subagent result lost the reply");
        NodeAssert.equal(child.thread.lineage.relationshipToParent, "subagent");
        return { childThreadId: childId, roles, result: done.value.result };
      });
    });
  }).pipe(Effect.scoped, Effect.provide(layerProtocol), Effect.timeout(timeoutMs));
  await Effect.runPromise(program, { signal: abort.signal });
} catch (error) {
  verdicts.harness = { status: "failed", detail: String(error) };
  console.error(String(error));
} finally {
  NodeFS.writeFileSync(NodePath.join(evidence, "verdict.json"), JSON.stringify(verdicts, null, 2));
  console.log(JSON.stringify(verdicts, null, 2));
  process.exitCode = Object.values(verdicts).some((verdict) => verdict.status === "failed") ? 1 : 0;
  if (server.pid) {
    try {
      process.kill(-server.pid, "SIGTERM");
      await sleep(3000);
      process.kill(-server.pid, "SIGKILL");
    } catch {
      // already gone
    }
  }
  process.off("SIGINT", onSignal);
  process.off("SIGTERM", onSignal);
}
