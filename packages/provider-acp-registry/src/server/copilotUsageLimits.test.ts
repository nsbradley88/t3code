import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import { copilotUserToAccount, readCopilotAccount } from "./copilotUsageLimits.ts";

const checkedAt = "2026-11-12T03:20:00.000Z";

// Shape of GET /copilot_internal/user as observed with Copilot CLI 1.0.109, identifiers scrubbed.
const quota = (entitlement: number, percentRemaining: number, unlimited = false) => ({
  entitlement,
  remaining: (entitlement * percentRemaining) / 100,
  percent_remaining: percentRemaining,
  quota_remaining: (entitlement * percentRemaining) / 100,
  unlimited,
  overage_permitted: false,
  overage_count: 0,
  has_quota: entitlement > 0,
  quota_reset_at: 0,
  timestamp_utc: checkedAt,
});
const enterpriseUser = {
  id: 1001,
  login: "octo-user",
  access_type_sku: "copilot_enterprise_seat_quota",
  copilot_plan: "enterprise",
  chat_enabled: true,
  quota_reset_date: "2026-12-01",
  quota_reset_date_utc: "2026-12-01T00:00:00.000Z",
  quota_snapshots: {
    chat: quota(0, 100, true),
    completions: quota(0, 100, true),
    premium_interactions: { ...quota(1_000_000, 28.1), overage_permitted: true },
  },
};
const freeUser = {
  id: 1002,
  login: "octo-free",
  access_type_sku: "free_limited_copilot",
  copilot_plan: "individual",
  quota_reset_date: "2026-12-01",
  quota_snapshots: {
    chat: quota(200, 75),
    completions: quota(2000, 100),
    premium_interactions: quota(0, 0),
  },
};

describe("copilotUserToAccount", () => {
  it("maps limited quotas to monthly windows that reset with the quota", () => {
    expect(copilotUserToAccount(enterpriseUser, "github.com", checkedAt)).toEqual({
      plan: "Copilot Enterprise",
      usageLimits: {
        checkedAt,
        credentialFingerprint: "github.com:1001",
        windows: [
          {
            id: "premium",
            kind: "monthly",
            label: "Premium requests",
            usedPercent: 71.9,
            resetsAt: "2026-12-01T00:00:00.000Z",
          },
        ],
      },
    });
    // A plan without premium requests is not one that spent them all.
    expect(copilotUserToAccount(freeUser, "github.com", checkedAt)).toMatchObject({
      plan: "Copilot Free",
      usageLimits: {
        windows: [
          {
            id: "chat",
            kind: "monthly",
            label: "Chat",
            usedPercent: 25,
            resetsAt: "2026-12-01T00:00:00.000Z",
          },
        ],
      },
    });
  });

  it("reports unlimited plans and accounts without Copilot as unavailable", () => {
    const unlimited = copilotUserToAccount(
      {
        ...enterpriseUser,
        quota_snapshots: { chat: quota(0, 100, true), premium_interactions: quota(0, 100, true) },
      },
      "github.com",
      checkedAt,
    );
    expect(unlimited.usageLimits.windows).toEqual([]);
    expect(unlimited.usageLimits.unavailable?.reason).toBe("unsupported");
    const withoutCopilot = copilotUserToAccount(
      { id: 1003, login: "octo-none", copilot_plan: "individual" },
      "example.ghe.com",
      checkedAt,
    );
    expect(withoutCopilot.plan).toBeUndefined();
    expect(withoutCopilot.usageLimits.unavailable?.reason).toBe("probeFailed");
  });
});

describe("readCopilotAccount", () => {
  const writeHome = Effect.fn("writeHome")(function* (authTokens: Record<string, unknown>) {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-copilot-home-" });
    yield* fileSystem.writeFileString(
      path.join(home, "config.json"),
      `// User settings belong in settings.json.\n// This file is managed automatically.\n${JSON.stringify(
        {
          authTokens,
          loggedInUsers: [
            { host: "https://github.com", login: "octo-dotcom" },
            { host: "https://example.ghe.com", login: "octo-user" },
          ],
          lastLoggedInUser: { host: "https://github.com", login: "octo-dotcom" },
        },
      )}`,
    );
    // The fake gh runs on the real host, so keep the host PATH behind it.
    yield* fileSystem.writeFileString(path.join(home, "gh"), "#!/bin/sh\necho gh-token\n");
    yield* fileSystem.chmod(path.join(home, "gh"), 0o755);
    const hostPath = HostProcess.Environment.defaultValue().PATH ?? "";
    return {
      COPILOT_HOME: home,
      COPILOT_GH_HOST: "example.ghe.com",
      PATH: `${home}:${hostPath}`,
    };
  });
  const respondWith = (requests: Array<readonly [string, string | undefined]>, body: unknown) =>
    HttpClient.make((request) => {
      requests.push([request.url, request.headers.authorization]);
      return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(body)));
    });

  it.effect("reads the Copilot CLI login for the configured host", () =>
    Effect.gen(function* () {
      const environment = yield* writeHome({
        "https://github.com:octo-dotcom": { token: "dotcom-token" },
        "https://example.ghe.com:octo-user": { token: "ghe-token" },
      });
      const requests: Array<readonly [string, string | undefined]> = [];
      const account = yield* readCopilotAccount(environment).pipe(
        Effect.provideService(HttpClient.HttpClient, respondWith(requests, enterpriseUser)),
      );
      expect(requests).toEqual([
        ["https://api.example.ghe.com/copilot_internal/user", "token ghe-token"],
      ]);
      expect(account.usageLimits.windows.map((window) => window.id)).toEqual(["premium"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects a GitHub CLI token for a different account than Copilot's", () =>
    Effect.gen(function* () {
      // Keychain-backed logins leave only the login name in config.json.
      const environment = yield* writeHome({});
      const requests: Array<readonly [string, string | undefined]> = [];
      const account = yield* readCopilotAccount(environment).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          respondWith(requests, { ...enterpriseUser, login: "someone-else" }),
        ),
      );
      expect(requests[0]?.[1]).toBe("token gh-token");
      expect(account.plan).toBeUndefined();
      expect(account.usageLimits.windows).toEqual([]);
      expect(account.usageLimits.unavailable?.reason).toBe("probeFailed");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("prefers token environment variables and hides failed responses", () =>
    Effect.gen(function* () {
      const requests: Array<readonly [string, string | undefined]> = [];
      const account = yield* readCopilotAccount({
        COPILOT_GITHUB_TOKEN: "env-token",
        GH_TOKEN: "ignored",
      }).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) => {
            requests.push([request.url, request.headers.authorization]);
            return Effect.succeed(
              HttpClientResponse.fromWeb(request, new Response("private body", { status: 404 })),
            );
          }),
        ),
      );
      expect(requests).toEqual([
        ["https://api.github.com/copilot_internal/user", "token env-token"],
      ]);
      expect(account.usageLimits.unavailable).toEqual({
        reason: "probeFailed",
        message: "GitHub Copilot could not read usage limits.",
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
