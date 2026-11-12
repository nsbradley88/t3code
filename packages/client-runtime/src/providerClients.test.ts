import { resolveProviderInstanceEnabled } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { providerClients } from "./providerClients.ts";

describe("providerClients", () => {
  it("resolves an unconfigured instance's enabled state to its settings default", () => {
    for (const { driverKind, settingsSchema } of providerClients.definitions) {
      const defaults = Schema.decodeSync(
        settingsSchema as unknown as Schema.Codec<{ readonly enabled?: boolean }>,
      )({});
      expect([driverKind, resolveProviderInstanceEnabled({ driver: driverKind })]).toEqual([
        driverKind,
        defaults.enabled ?? true,
      ]);
    }
  });
});
