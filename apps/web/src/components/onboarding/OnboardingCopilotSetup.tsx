import { useRef, useState } from "react";
import {
  AuthSettingsWriteScope,
  defaultInstanceIdForDriver,
  isManagedSettingPath,
  ProviderDriverKind,
  resolveProviderInstanceEnabled,
  type EnvironmentId,
  type ServerConfig,
  type ServerProvider,
} from "@t3tools/contracts";

import { getOnboardingProviderState } from "../../onboarding/providerReadiness.logic";
import { managedInstanceLocks } from "../../state/managedPolicy";
import { readEnvironmentScope, useEnvironmentScope } from "../../state/session";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { ProviderInstanceIcon } from "../chat/ProviderInstanceIcon";
import { ManagedControl } from "../ManagedPolicy";
import { ProviderAuthenticationSection } from "../settings/ProviderAuthenticationSection";
import { providerClients } from "../settings/providerDriverMeta";
import { getProviderSummary } from "../settings/providerStatus";
import { ProviderSettingsForm } from "../settings/ProviderSettingsForm";
import { buildProviderInstanceUpdatePatch } from "../settings/SettingsPanels.logic";
import { SettingsRow } from "../settings/settingsLayout";
import { Switch } from "../ui/switch";

const driver = ProviderDriverKind.make("copilot");
const definition = providerClients.get(driver);

export function OnboardingCopilotSetup({
  environmentId,
  environmentLabel,
  provider,
  serverConfig,
}: {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
  readonly provider: ServerProvider | undefined;
  readonly serverConfig: ServerConfig | null;
}) {
  const canWriteSettings = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const update = useAtomCommand(serverEnvironment.updateSettings, "Copilot setup settings");
  const refresh = useAtomCommand(serverEnvironment.refreshProviders, "Copilot setup readiness");
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);
  const instanceId = provider?.instanceId ?? defaultInstanceIdForDriver(driver);
  const instance = serverConfig?.settings.providerInstances[instanceId] ?? {
    driver,
    enabled: false,
  };
  const policy = serverConfig?.managedPolicy;
  const locks = managedInstanceLocks(policy, driver);
  const configLocks = new Set([
    ...(locks?.configKeys ?? []),
    ...Object.keys(definition?.settingsSchema.fields ?? {}).filter((key) =>
      isManagedSettingPath(policy, `providerInstances.${instanceId}.config.${key}`),
    ),
  ]);
  const managedReason = policy ? `Managed by ${policy.organization}.` : undefined;
  const enabledLocked =
    configLocks.has("enabled") ||
    isManagedSettingPath(policy, `providerInstances.${instanceId}.enabled`);
  const enabled = provider?.enabled ?? resolveProviderInstanceEnabled(instance);
  const readOnly = saving || !canWriteSettings || serverConfig === null;
  const summary = getProviderSummary(provider && { ...provider, enabled });
  const state = getOnboardingProviderState(provider && { ...provider, enabled });
  const displayName = provider?.displayName || "GitHub Copilot";

  async function persist(next: typeof instance) {
    if (
      savingRef.current ||
      serverConfig === null ||
      !readEnvironmentScope(environmentId, AuthSettingsWriteScope)
    )
      return;
    savingRef.current = true;
    setSaving(true);
    try {
      const result = await update({
        environmentId,
        input: {
          patch: buildProviderInstanceUpdatePatch({
            settings: serverConfig.settings,
            instanceId,
            instance: next,
          }),
        },
      });
      if (result._tag === "Success") await refresh({ environmentId, input: {} });
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  }

  return (
    <section
      aria-label={`${displayName} setup`}
      className="rounded-lg border border-border bg-background px-4 py-4"
    >
      <div className="flex items-center gap-3">
        <ProviderInstanceIcon
          driverKind={driver}
          displayName={displayName}
          iconClassName="size-5"
        />
        <div className="min-w-0 flex-1">
          <h3 className="text-sm font-medium">{displayName}</h3>
          <p role="status" className="mt-0.5 text-xs text-muted-foreground">
            {saving
              ? "Saving and checking Copilot..."
              : state === "ready"
                ? "Ready to code."
                : summary.headline}
            {!saving && state !== "ready" && summary.detail ? ` · ${summary.detail}` : ""}
          </p>
        </div>
        <ManagedControl reason={enabledLocked ? managedReason : undefined}>
          <Switch
            aria-label={`Enable ${displayName}`}
            checked={enabled}
            disabled={readOnly || enabledLocked}
            onCheckedChange={(value) =>
              void persist({
                ...instance,
                enabled: value,
                config: {
                  ...(instance.config !== null && typeof instance.config === "object"
                    ? instance.config
                    : {}),
                  enabled: value,
                },
              })
            }
          />
        </ManagedControl>
      </div>
      {!canWriteSettings ? (
        <p className="mt-2 text-xs text-muted-foreground">
          This connection cannot change provider settings.
        </p>
      ) : null}
      {definition && serverConfig ? (
        <div className="mt-3 divide-y divide-border/50 text-xs">
          <div inert={readOnly} aria-disabled={readOnly || undefined}>
            <ProviderSettingsForm
              definition={definition}
              value={instance.config}
              idPrefix={`onboarding-copilot-${environmentId}-${instanceId}`}
              variant="settings"
              environmentId={environmentId}
              managed={managedReason ? { keys: configLocks, reason: managedReason } : undefined}
              onChange={(config) => void persist({ ...instance, config })}
            />
          </div>
          {enabled && provider?.installed ? (
            <ProviderAuthenticationSection
              key={`${environmentId}:${instanceId}`}
              environmentId={environmentId}
              environmentLabel={environmentLabel}
              instanceId={instanceId}
              provider={provider}
              readOnly={saving}
            />
          ) : state === "install" ? (
            <SettingsRow
              title="Install GitHub Copilot CLI"
              description={`Install Copilot on ${environmentLabel}, or set its binary path above.`}
              control={
                <a
                  href={
                    provider?.setup?.documentationUrl ?? "https://github.com/github/copilot-cli"
                  }
                  target="_blank"
                  rel="noreferrer"
                  className="text-xs underline"
                >
                  Installation instructions
                </a>
              }
            />
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
