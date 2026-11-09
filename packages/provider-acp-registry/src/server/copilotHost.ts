import type { CopilotSettings } from "../settings.ts";

export function resolveCopilotEnvironment(
  settings: CopilotSettings,
  environment: NodeJS.ProcessEnv,
): NodeJS.ProcessEnv {
  return { ...environment, COPILOT_GH_HOST: settings.githubHost.toLowerCase() };
}

export function resolveCopilotLoginArgs(
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
): ReadonlyArray<string> {
  const host = environment.COPILOT_GH_HOST ?? environment.GH_HOST;
  if (!host) return args;
  const withoutHost: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg === "--host") index += 1;
    else if (!arg.startsWith("--host=")) withoutHost.push(arg);
  }
  return [...withoutHost, "--host", `https://${host}`];
}
