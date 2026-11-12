import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import {
  clampPercent,
  makeUnavailableUsageLimits,
  makeUsageLimits,
} from "@t3tools/provider-core/server/usageLimits";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

const CopilotQuotaSnapshot = Schema.Struct({
  entitlement: Schema.optional(Schema.NullOr(Schema.Number)),
  percent_remaining: Schema.optional(Schema.NullOr(Schema.Number)),
  unlimited: Schema.optional(Schema.NullOr(Schema.Boolean)),
});

export const CopilotUserResponse = Schema.Struct({
  id: Schema.optional(Schema.NullOr(Schema.Number)),
  login: Schema.optional(Schema.NullOr(Schema.String)),
  access_type_sku: Schema.optional(Schema.NullOr(Schema.String)),
  copilot_plan: Schema.optional(Schema.NullOr(Schema.String)),
  quota_reset_date: Schema.optional(Schema.NullOr(Schema.String)),
  quota_reset_date_utc: Schema.optional(Schema.NullOr(Schema.String)),
  quota_snapshots: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        premium_interactions: Schema.optional(Schema.NullOr(CopilotQuotaSnapshot)),
        chat: Schema.optional(Schema.NullOr(CopilotQuotaSnapshot)),
      }),
    ),
  ),
});
export type CopilotUserResponse = typeof CopilotUserResponse.Type;

const CopilotConfig = Schema.Struct({
  authTokens: Schema.optional(
    Schema.Record(Schema.String, Schema.Struct({ token: Schema.optional(Schema.String) })),
  ),
  loggedInUsers: Schema.optional(
    Schema.Array(Schema.Struct({ host: Schema.String, login: Schema.String })),
  ),
  lastLoggedInUser: Schema.optional(Schema.Struct({ host: Schema.String, login: Schema.String })),
});
const decodeCopilotConfig = Schema.decodeUnknownOption(Schema.fromJsonString(CopilotConfig));

// Inline completions are an editor quota; agent turns spend premium requests and chat.
const COPILOT_USAGE_WINDOWS = [
  { key: "premium_interactions", id: "premium", label: "Premium requests" },
  { key: "chat", id: "chat", label: "Chat" },
] as const;

export interface CopilotAccount {
  readonly plan: string | undefined;
  readonly usageLimits: ServerProviderUsageLimits;
}

function copilotPlanLabel(user: CopilotUserResponse): string | undefined {
  const sku = user.access_type_sku ?? "";
  if (sku.includes("free")) return "Copilot Free";
  switch (user.copilot_plan) {
    case "enterprise":
      return "Copilot Enterprise";
    case "business":
      return "Copilot Business";
    case "individual":
      return sku.includes("plus") ? "Copilot Pro+" : "Copilot Pro";
    default:
      return undefined;
  }
}

export function copilotUserToAccount(
  user: CopilotUserResponse,
  host: string,
  checkedAt: string,
): CopilotAccount {
  const snapshots = user.quota_snapshots;
  if (!snapshots) {
    return {
      plan: undefined,
      usageLimits: makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: "GitHub did not report Copilot quotas for this account.",
      }),
    };
  }
  const reset = Option.orElse(DateTime.make(user.quota_reset_date_utc ?? ""), () =>
    user.quota_reset_date ? DateTime.make(`${user.quota_reset_date}T00:00:00Z`) : Option.none(),
  );
  const resetsAt = Option.isSome(reset) ? DateTime.formatIso(reset.value) : undefined;
  const windows: ServerProviderUsageWindow[] = [];
  for (const { key, id, label } of COPILOT_USAGE_WINDOWS) {
    const quota = snapshots[key];
    // A zero entitlement is a quota the plan does not include, not one already spent.
    if (!quota || quota.unlimited || !((quota.entitlement ?? 0) > 0)) continue;
    if (typeof quota.percent_remaining !== "number") continue;
    windows.push({
      id,
      kind: "monthly",
      label,
      usedPercent: clampPercent(100 - quota.percent_remaining),
      ...(resetsAt ? { resetsAt } : {}),
    });
  }
  return {
    plan: copilotPlanLabel(user),
    usageLimits: {
      ...(windows.length > 0
        ? makeUsageLimits({ checkedAt, windows })
        : makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" })),
      ...(typeof user.id === "number" ? { credentialFingerprint: `${host}:${user.id}` } : {}),
    },
  };
}

interface CopilotCredential {
  readonly token: string;
  /** The Copilot CLI login the token must belong to, when known. */
  readonly expectedLogin: string | undefined;
}

/** Mirrors Copilot CLI's lookup: token env vars, then its stored login, then GitHub CLI. */
const readCopilotCredential = Effect.fn("readCopilotCredential")(function* (
  host: string,
  environment: NodeJS.ProcessEnv,
) {
  for (const name of ["COPILOT_GITHUB_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"]) {
    const token = environment[name]?.trim();
    if (token) return { token, expectedLogin: undefined } satisfies CopilotCredential;
  }
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home =
    environment.COPILOT_HOME?.trim() || path.join(yield* HostProcess.HomeDirectory, ".copilot");
  const contents = yield* fileSystem
    .readFileString(path.join(home, "config.json"))
    .pipe(Effect.orElseSucceed(() => ""));
  // The CLI writes `//` header comments above the JSON body.
  const config = decodeCopilotConfig(
    contents
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("//"))
      .join("\n"),
  );
  const origin = `https://${host}`;
  const login = Option.match(config, {
    onNone: () => undefined,
    onSome: ({ lastLoggedInUser, loggedInUsers }) =>
      lastLoggedInUser?.host === origin
        ? lastLoggedInUser.login
        : loggedInUsers?.find((user) => user.host === origin)?.login,
  });
  const stored =
    login === undefined
      ? undefined
      : Option.getOrUndefined(config)?.authTokens?.[`${origin}:${login}`]?.token?.trim();
  if (stored) return { token: stored, expectedLogin: login } satisfies CopilotCredential;
  // With a system keychain the CLI keeps only the login here; gh usually shares it.
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const ghToken = yield* spawner
    .string(
      ChildProcess.make("gh", ["auth", "token", "--hostname", host], {
        env: environment,
        stdin: "ignore",
        stderr: "ignore",
      }),
    )
    .pipe(
      Effect.map((output) => output.trim()),
      Effect.orElseSucceed(() => ""),
    );
  return ghToken ? ({ token: ghToken, expectedLogin: login } satisfies CopilotCredential) : null;
});

export const readCopilotAccount = Effect.fn("readCopilotAccount")(function* (
  environment: NodeJS.ProcessEnv,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const host = (environment.COPILOT_GH_HOST ?? environment.GH_HOST ?? "github.com")
    .trim()
    .toLowerCase();
  const probeFailed = (message: string): CopilotAccount => ({
    plan: undefined,
    usageLimits: makeUnavailableUsageLimits({ checkedAt, reason: "probeFailed", message }),
  });
  return yield* Effect.gen(function* () {
    const credential = yield* readCopilotCredential(host, environment);
    if (credential === null) return probeFailed("Sign in to GitHub Copilot to show usage limits.");
    const client = yield* HttpClient.HttpClient;
    const apiBase = host === "github.com" ? "https://api.github.com" : `https://api.${host}`;
    const response = yield* client.execute(
      HttpClientRequest.get(`${apiBase}/copilot_internal/user`).pipe(
        HttpClientRequest.setHeaders({
          Authorization: `token ${credential.token}`,
          Accept: "application/json",
          "User-Agent": "t3code",
        }),
      ),
    );
    const user = yield* HttpClientResponse.schemaBodyJson(CopilotUserResponse)(
      yield* HttpClientResponse.filterStatusOk(response),
    );
    if (
      credential.expectedLogin !== undefined &&
      user.login?.toLowerCase() !== credential.expectedLogin.toLowerCase()
    ) {
      return probeFailed(
        `GitHub CLI is signed in to a different account than Copilot (${credential.expectedLogin}).`,
      );
    }
    return copilotUserToAccount(user, host, checkedAt);
  }).pipe(
    Effect.timeout("10 seconds"),
    Effect.orElseSucceed(() => probeFailed("GitHub Copilot could not read usage limits.")),
  );
});
