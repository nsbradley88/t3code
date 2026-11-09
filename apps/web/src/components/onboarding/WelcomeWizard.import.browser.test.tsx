import { RegistryContext } from "@effect/atom-react";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  resolveProviderInstanceEnabled,
  type AgentSessionProjectCandidate,
  type AuthEnvironmentScope,
  type AuthSessionState,
  type ProviderAuthState,
  type ServerConfig,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";
import { act, render } from "../../../test/render";

type SessionResult = AsyncResult.AsyncResult<AuthSessionState, Error>;
type OnboardingConfig = Pick<ServerConfig, "settings" | "managedPolicy">;
type Project = { id: ProjectId; environmentId: EnvironmentId; workspaceRoot: string };
type CreateProjectInput = {
  environmentId: EnvironmentId;
  input: { projectId: ProjectId; workspaceRoot: string };
};
const state = vi.hoisted(() => ({
  selectedEnvironment: "primary",
  addedEnvironments: [] as string[],
  sessions: new Map<string, unknown>(),
  projects: null as unknown,
  providers: null as unknown,
  config: null as unknown,
  configs: new Map<EnvironmentId, unknown>(),
  auth: null as unknown,
  candidates: [] as AgentSessionProjectCandidate[],
  createProject: vi.fn(),
  importThreads: vi.fn(),
  refreshProviders: vi.fn(),
  updateSettings: vi.fn(),
  startProviderAuth: vi.fn(),
  connectPairing: vi.fn(),
  completeOnboarding: vi.fn(),
  onDone: vi.fn(),
  scan: vi.fn(),
  refreshScan: vi.fn(),
  waitForProject: vi.fn(),
}));

vi.mock(import("@t3tools/client-runtime/state/session"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    createEnvironmentSessionAtoms: ((runtime: never) => ({
      ...actual.createEnvironmentSessionAtoms(runtime),
      sessionStateAtom: (id: EnvironmentId) => state.sessions.get(id),
    })) as never,
  };
});
vi.mock(import("../../cloud/publicConfig"), async (original) => ({
  ...(await original()),
  hasCloudPublicConfig: () => false,
}));
vi.mock(import("../../onboarding/firstRun"), async (original) => ({
  ...(await original()),
  completeOnboarding: state.completeOnboarding,
}));
vi.mock(import("../../state/environments"), async (original) => {
  const environment = (environmentId: string) => ({
    environmentId,
    label: environmentId,
    connection: { phase: "connected" },
    entry: { enabled: true, target: { _tag: "DirectConnectionTarget" } },
  });
  const primary = environment("primary");
  return {
    ...(await original()),
    usePrimaryEnvironment: (() => primary) as never,
    useEnvironments: (() => ({
      environments: [state.selectedEnvironment, ...state.addedEnvironments].map(environment),
    })) as never,
  };
});
vi.mock(import("../../state/server"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    serverEnvironment: {
      ...actual.serverEnvironment,
      providersValueAtom: (() => state.providers) as never,
      configValueAtom: ((id: EnvironmentId) => state.configs.get(id) ?? state.config) as never,
      providerAuthState: (() => state.auth) as never,
      refreshProviders: "refreshProviders" as never,
      updateSettings: "updateSettings" as never,
      startProviderAuth: "startProviderAuth" as never,
    },
  };
});
vi.mock(import("../../state/entities"), async (original) => {
  const { useAtomValue } = await import("@effect/atom-react");
  return {
    ...(await original()),
    useProjects: (() => useAtomValue(state.projects as Atom.Atom<Project[]>)) as never,
    readProjects: (() => appAtomRegistry.get(state.projects as Atom.Atom<Project[]>)) as never,
    waitForProject: ((ref: unknown) => state.waitForProject(ref)) as never,
  };
});
vi.mock(import("../../state/projects"), async (original) => ({
  ...(await original()),
  projectEnvironment: { create: "createProject" } as never,
}));
vi.mock(import("../../state/agentSessions"), async (original) => ({
  ...(await original()),
  agentSessionImport: "importThreads" as never,
}));
vi.mock(import("../../onboarding/useProjectScans"), async (original) => ({
  ...(await original()),
  useProjectScans: (ids: readonly EnvironmentId[]) =>
    ids.map((environmentId) => {
      state.scan({ environmentId });
      return {
        environmentId,
        data: { candidates: state.candidates, truncated: false } as never,
        isPending: false,
        error: null,
        refresh: state.refreshScan,
      };
    }),
}));
vi.mock(import("../../connection/onboarding"), async (original) => ({
  ...(await original()),
  connectPairing: "connectPairing" as never,
}));
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((
    command:
      | "createProject"
      | "importThreads"
      | "refreshProviders"
      | "connectPairing"
      | "updateSettings"
      | "startProviderAuth",
  ) => state[command]) as never,
}));
vi.mock(import("../settings/ChatGptWelcomeCoordinator"), async (original) => ({
  ...(await original()),
  ChatGptWelcomeCoordinator: () => <></>,
}));

import { appAtomRegistry } from "../../rpc/atomRegistry";
import { WelcomeWizard } from "./WelcomeWizard";

const primaryId = EnvironmentId.make("primary");
const remoteId = EnvironmentId.make("remote");
const permissionMessage = "This connection cannot import projects or thread history.";

function session(scopes: ReadonlyArray<AuthEnvironmentScope>): AuthSessionState {
  return {
    authenticated: true,
    scopes: [AuthOrchestrationReadScope, ...scopes],
    auth: {
      policy: "remote-reachable",
      bootstrapMethods: ["one-time-token"],
      sessionMethods: ["bearer-access-token"],
      sessionCookieName: "t3_session",
    },
  };
}

function sessionAtom(environmentId: EnvironmentId) {
  return state.sessions.get(environmentId) as Atom.Writable<SessionResult>;
}

function projectsAtom() {
  return state.projects as Atom.Writable<ReadonlyArray<Project>>;
}

function setGrant(environmentId: EnvironmentId, allowed: boolean) {
  appAtomRegistry.set(
    sessionAtom(environmentId),
    AsyncResult.success(session(allowed ? [AuthOrchestrationOperateScope] : [])),
  );
}

// Default selection only picks recently active projects, so pin "now".
const NOW = Date.parse("2026-01-01T00:00:00.000Z");

function candidate(path: string): AgentSessionProjectCandidate {
  return {
    path,
    title: path.split("/").at(-1)!,
    sources: ["codex"],
    threadCount: 10,
    lastActiveAt: new Date(NOW - 60_000).toISOString(),
    alreadyImported: false,
  };
}

function recordProject({ environmentId, input }: CreateProjectInput) {
  appAtomRegistry.set(projectsAtom(), [
    ...appAtomRegistry.get(projectsAtom()),
    { id: input.projectId, environmentId, workspaceRoot: input.workspaceRoot },
  ]);
  return AsyncResult.success(undefined);
}

// Resolves once the project reaches the mocked projects atom; the real helper's timeout is
// replaced per test with a rejection.
function waitForRecordedProject(ref: { environmentId: EnvironmentId; projectId: string }) {
  const find = () =>
    appAtomRegistry
      .get(projectsAtom())
      .find(
        (project) => project.id === ref.projectId && project.environmentId === ref.environmentId,
      );
  return new Promise<Project>((resolve) => {
    const current = find();
    if (current) return resolve(current);
    const unsubscribe = appAtomRegistry.subscribe(projectsAtom(), () => {
      const project = find();
      if (!project) return;
      unsubscribe();
      resolve(project);
    });
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function pageText() {
  return document.body.textContent ?? "";
}

function button(label: string) {
  const found = [...document.querySelectorAll("button")].find(
    (element) => element.textContent?.trim() === label,
  );
  if (!found) throw new Error(`Missing button: ${label}`);
  return found;
}

async function click(label: string) {
  const target = button(label);
  expect(target.disabled).toBe(false);
  await act(async () => target.click());
}

async function clickStage(label: string) {
  const stage = document.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
  expect(stage?.disabled).toBe(false);
  await act(async () => stage?.click());
}

async function mountImport(remote = false) {
  state.selectedEnvironment = remote ? "remote" : "primary";
  await using stack = new AsyncDisposableStack();
  const screen = stack.use(
    await render(
      <RegistryContext.Provider value={appAtomRegistry}>
        <WelcomeWizard localAvailable={!remote} onDone={state.onDone} />
      </RegistryContext.Provider>,
    ),
  );
  await click("Continue");
  await click("Continue");
  expect(pageText()).toContain("Import your projects");
  stack.move();
  return screen;
}

// The mocked modules read `state`, so every test starts by replacing all of it.
// Returns the clock spy so each test can `using` it; the spy would otherwise outlive the test.
function setup() {
  const now = vi.spyOn(Date, "now").mockReturnValue(NOW);
  state.addedEnvironments = [];
  state.configs.clear();
  state.sessions.clear();
  for (const id of [primaryId, remoteId]) {
    state.sessions.set(
      id,
      Atom.make<SessionResult>(AsyncResult.success(session([AuthOrchestrationOperateScope]))).pipe(
        Atom.keepAlive,
      ),
    );
  }
  state.projects = Atom.make<ReadonlyArray<Project>>([]).pipe(Atom.keepAlive);
  state.providers = Atom.make<ReadonlyArray<ServerProvider>>([]).pipe(Atom.keepAlive);
  state.config = Atom.make(null).pipe(Atom.keepAlive);
  state.auth = Atom.make(AsyncResult.initial<ProviderAuthState>()).pipe(Atom.keepAlive);
  state.candidates = [candidate("/projects/first"), candidate("/projects/second")];
  state.createProject
    .mockReset()
    .mockImplementation(async (input: CreateProjectInput) => recordProject(input));
  state.importThreads
    .mockReset()
    .mockResolvedValue(AsyncResult.success({ importedCount: 1, skippedCount: 0 }));
  state.refreshProviders.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.updateSettings.mockReset();
  state.startProviderAuth.mockReset();
  state.connectPairing.mockReset().mockResolvedValue(AsyncResult.success(remoteId));
  state.completeOnboarding.mockReset().mockResolvedValue(undefined);
  state.onDone.mockReset();
  state.scan.mockReset();
  state.refreshScan.mockReset();
  state.waitForProject.mockReset().mockImplementation(waitForRecordedProject);
  return now;
}

it("connects Copilot on the selected computer without losing its host or binary path", async () => {
  using _now = setup();
  state.selectedEnvironment = "remote";
  const instanceId = ProviderInstanceId.make("copilot");
  const driver = ProviderDriverKind.make("copilot");
  const binaryPath = "C:\\Tools\\GitHub Copilot\\copilot.exe";
  const updatedBinaryPath = "C:\\Tools\\Copilot Native\\copilot.exe";
  const providers = state.providers as Atom.Writable<ReadonlyArray<ServerProvider>>;
  const auth = state.auth as Atom.Writable<AsyncResult.AsyncResult<ProviderAuthState>>;
  const remoteConfig = Atom.make<OnboardingConfig>({
    settings: {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [instanceId]: { driver, config: { enabled: false, commandPath: binaryPath } },
      },
    },
    managedPolicy: {
      organization: "Example Corp",
      lockedSettings: [],
      limits: { providers: { allowedDrivers: [driver] } },
    },
  }).pipe(Atom.keepAlive);
  const primaryConfig = Atom.make<OnboardingConfig>({
    settings: DEFAULT_SERVER_SETTINGS,
  }).pipe(Atom.keepAlive);
  state.configs.set(remoteId, remoteConfig);
  state.configs.set(primaryId, primaryConfig);
  appAtomRegistry.set(
    sessionAtom(remoteId),
    AsyncResult.success(session([AuthSettingsWriteScope])),
  );
  const copilot: ServerProvider = {
    instanceId,
    driver,
    enabled: false,
    installed: true,
    version: "test-version",
    status: "disabled",
    auth: { status: "unauthenticated" },
    checkedAt: "2026-11-09T00:00:00.000Z",
    models: [],
    skills: [],
    slashCommands: [],
    setup: { canAuthenticate: true, canInstall: false },
  };
  appAtomRegistry.set(providers, [copilot]);
  const idleAuth: ProviderAuthState = {
    instanceId,
    phase: "idle",
    flowId: null,
    authorizationUrl: null,
    expiresAt: null,
    message: null,
    methods: [{ id: "github", name: "GitHub", description: null, type: "agent" }],
  };
  appAtomRegistry.set(auth, AsyncResult.success(idleAuth));
  state.updateSettings.mockImplementation(
    async ({
      environmentId,
      input,
    }: {
      environmentId: EnvironmentId;
      input: { patch: Partial<ServerSettings> };
    }) => {
      const config = state.configs.get(environmentId) as Atom.Writable<OnboardingConfig>;
      const current = appAtomRegistry.get(config);
      appAtomRegistry.set(config, {
        ...current,
        settings: { ...current.settings, ...input.patch },
      });
      return AsyncResult.success(undefined);
    },
  );
  state.refreshProviders.mockImplementation(
    async ({ environmentId }: { environmentId: EnvironmentId }) => {
      const config = state.configs.get(environmentId) as Atom.Writable<OnboardingConfig>;
      const instance = appAtomRegistry.get(config).settings.providerInstances[instanceId];
      const enabled = instance !== undefined && resolveProviderInstanceEnabled(instance);
      appAtomRegistry.set(providers, [
        { ...copilot, enabled, status: enabled ? "error" : "disabled" },
      ]);
      return AsyncResult.success(undefined);
    },
  );
  state.startProviderAuth.mockImplementation(async () => {
    appAtomRegistry.set(
      auth,
      AsyncResult.success({
        ...idleAuth,
        phase: "waiting",
        flowId: "copilot-sign-in",
        interaction: {
          type: "deviceCode",
          id: "github-device-code",
          url: "https://example.ghe.com/login/device",
          userCode: "ABCD-EFGH",
        },
      }),
    );
    return AsyncResult.success(undefined);
  });
  await using _screen = await render(
    <RegistryContext.Provider value={appAtomRegistry}>
      <WelcomeWizard localAvailable={false} resumeEnvironmentId={remoteId} onDone={state.onDone} />
    </RegistryContext.Provider>,
  );
  const setupSection = page.getByRole("region", { name: "GitHub Copilot setup" });
  await expect.element(page.getByRole("heading", { name: "Connect your agents" })).toBeVisible();
  await expect.element(setupSection).toBeVisible();
  await expect
    .element(setupSection.getByRole("button", { name: "Sign in", exact: true }))
    .not.toBeInTheDocument();

  const host = setupSection.getByRole("textbox", { name: "GitHub host" });
  const binary = setupSection.getByRole("textbox", { name: "Binary path" });
  const toggle = setupSection.getByRole("switch", { name: "Enable GitHub Copilot" });
  await userEvent.fill(host, "example.ghe.com");
  await userEvent.keyboard("{Enter}");
  await vi.waitFor(() =>
    expect(
      appAtomRegistry.get(remoteConfig).settings.providerInstances[instanceId]?.config,
    ).toEqual({
      enabled: false,
      commandPath: binaryPath,
      githubHost: "example.ghe.com",
    }),
  );
  await toggle.click();
  await expect
    .element(setupSection.getByRole("button", { name: "Sign in", exact: true }))
    .toBeEnabled();
  await expect.element(setupSection.getByText("Sign in on remote.")).toBeVisible();
  expect(appAtomRegistry.get(remoteConfig).settings.providerInstances[instanceId]).toEqual({
    driver,
    enabled: true,
    config: { enabled: true, commandPath: binaryPath, githubHost: "example.ghe.com" },
  });

  await userEvent.fill(binary, updatedBinaryPath);
  await userEvent.keyboard("{Enter}");
  await vi.waitFor(() =>
    expect(
      appAtomRegistry.get(remoteConfig).settings.providerInstances[instanceId]?.config,
    ).toEqual({
      enabled: true,
      commandPath: updatedBinaryPath,
      githubHost: "example.ghe.com",
    }),
  );
  await expect.element(binary).toHaveValue(updatedBinaryPath);
  await expect.element(host).toHaveValue("example.ghe.com");
  await toggle.click();
  await expect.element(toggle).not.toBeChecked();
  await expect
    .element(setupSection.getByRole("button", { name: "Sign in", exact: true }))
    .not.toBeInTheDocument();
  await toggle.click();
  await expect
    .element(setupSection.getByRole("button", { name: "Sign in", exact: true }))
    .toBeEnabled();
  await setupSection.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect.element(setupSection.getByText("ABCD-EFGH", { exact: true })).toBeVisible();
  await expect.element(setupSection.getByRole("button", { name: "Open browser" })).toBeVisible();
  expect(appAtomRegistry.get(remoteConfig).settings.providerInstances[instanceId]).toEqual({
    driver,
    enabled: true,
    config: { enabled: true, commandPath: updatedBinaryPath, githubHost: "example.ghe.com" },
  });
  expect(appAtomRegistry.get(primaryConfig).settings).toEqual(DEFAULT_SERVER_SETTINGS);
});

it("keeps the project selection after going back a stage", async () => {
  using _now = setup();
  await using _screen = await mountImport();
  await click("Select none");
  expect(pageText()).toContain("0 of 2 selected");

  await clickStage("Agents, step 2");
  await click("Continue");
  expect(pageText()).toContain("Import your projects");
  expect(pageText()).toContain("0 of 2 selected");
});

it("ticks the recent projects of a computer added after the selection changed", async () => {
  using _now = setup();
  await using _screen = await mountImport();
  await click("Select none");

  state.addedEnvironments = ["remote"];
  await clickStage("Connect, step 1");
  await click("Continue");
  await click("Continue");
  expect(pageText()).toContain("2 of 4 selected");

  await click("Import 2 projects");
  const created = state.createProject.mock.calls as Array<[CreateProjectInput]>;
  expect(created.map(([call]) => call.environmentId)).toEqual([remoteId, remoteId]);
});

it("rechecks after project creation and can retry its history without recreating the project", async () => {
  using _now = setup();
  const creation = deferred<void>();
  state.createProject.mockImplementationOnce(async (input: CreateProjectInput) => {
    await creation.promise;
    return recordProject(input);
  });
  await using _screen = await mountImport();
  await click("Import 2 projects");
  expect(state.createProject).toHaveBeenCalledOnce();
  await act(async () => {
    setGrant(primaryId, false);
    creation.resolve();
    await creation.promise;
  });
  expect(state.importThreads).not.toHaveBeenCalled();
  expect(state.createProject).toHaveBeenCalledOnce();
  expect(state.onDone).not.toHaveBeenCalled();
  expect(pageText()).toContain(permissionMessage);

  await act(async () => setGrant(primaryId, true));
  await click("Import 2 projects");
  expect(state.createProject).toHaveBeenCalledTimes(2);
  expect(state.importThreads).toHaveBeenCalledTimes(2);
  expect(state.onDone).toHaveBeenCalledOnce();
});

it("completes accepted imports when permission is revoked after the final server write", async () => {
  using _now = setup();
  state.candidates = state.candidates.slice(0, 1);
  const imported = deferred<{ importedCount: number; skippedCount: number }>();
  state.importThreads.mockImplementationOnce(async () =>
    AsyncResult.success(await imported.promise),
  );
  await using _screen = await mountImport();
  await click("Import 1 project");
  await act(async () => {
    setGrant(primaryId, false);
    imported.resolve({ importedCount: 1, skippedCount: 0 });
    await imported.promise;
  });
  expect(state.createProject).toHaveBeenCalledOnce();
  expect(state.importThreads).toHaveBeenCalledOnce();
  expect(state.completeOnboarding).toHaveBeenCalledOnce();
  expect(state.onDone).toHaveBeenCalledOnce();
});

it("finishes setup without landing when the imported project never reaches the client", async () => {
  using _now = setup();
  state.candidates = state.candidates.slice(0, 1);
  // The server accepts the project, but it never shows up in the client's projects.
  state.createProject.mockResolvedValue(AsyncResult.success(undefined));
  state.waitForProject.mockRejectedValue(new Error("The project did not appear."));
  state.completeOnboarding.mockRejectedValueOnce(new Error("offline"));
  await using _screen = await mountImport();

  await click("Import 1 project");
  await vi.waitFor(() => expect(state.completeOnboarding).toHaveBeenCalledOnce());
  // A failed completion hands the step back instead of leaving it importing.
  await vi.waitFor(() => expect(button("Import 1 project").disabled).toBe(false));
  expect(state.onDone).not.toHaveBeenCalled();

  await click("Import 1 project");
  await vi.waitFor(() => expect(state.onDone).toHaveBeenCalledOnce());
  expect(state.onDone).toHaveBeenCalledWith(undefined);
});
