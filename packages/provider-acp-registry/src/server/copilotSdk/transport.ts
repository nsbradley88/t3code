import * as Effect from "effect/Effect";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { CopilotSdkCapabilitiesV2 } from "./adapter.ts";

/** `T3_COPILOT_TRANSPORT=acp` keeps Copilot on the ACP adapter. */
export function copilotSdkTransportDisabled(environment: NodeJS.ProcessEnv): boolean {
  return environment.T3_COPILOT_TRANSPORT?.trim().toLowerCase() === "acp";
}

type Adapter = ProviderAdapter.ProviderAdapterV2["Service"];

/**
 * Copilot runs on its SDK JSON-RPC server, which can steer a running turn and stop subagents.
 * When that server cannot start (an older CLI, a failed launch) the session opens on ACP, whose
 * adapter reports the capabilities it actually has.
 */
export function withCopilotSdkTransport(sdk: Adapter, acp: Adapter): Adapter {
  return ProviderAdapter.ProviderAdapterV2.of({
    ...acp,
    getCapabilities: Effect.fn("CopilotTransport.getCapabilities")(function () {
      return Effect.succeed(CopilotSdkCapabilitiesV2);
    }),
    openSession: Effect.fn("CopilotTransport.openSession")(function* (input) {
      return yield* sdk.openSession(input).pipe(
        Effect.catchTags({
          ProviderAdapterOpenSessionError: (error) =>
            Effect.logWarning("copilot.sdk-transport-unavailable; using ACP", {
              detail: error.cause instanceof Error ? error.cause.message : String(error.cause),
            }).pipe(Effect.andThen(acp.openSession(input))),
        }),
      );
    }),
  });
}
