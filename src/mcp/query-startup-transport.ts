import { captureQueryStartup, type QueryStartupRequest } from './query-policy';
import { QueryPolicyRefusalError } from './query-policy-refusal';

const CPU_METADATA = 'CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1';
const CONTROLS = ['CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS'] as const;

/** Copy the original request, including absence; never mutate the launcher's env. */
export function daemonQueryEnvironment(inherited: NodeJS.ProcessEnv, startup: QueryStartupRequest): NodeJS.ProcessEnv {
  const env = { ...inherited };
  const values = [startup.requested.mode, startup.requested.poolSize, startup.requested.busyTimeoutMs];
  CONTROLS.forEach((name, i) => {
    const value = values[i];
    if (value === undefined) delete env[name];
    else env[name] = value;
  });
  if (!Number.isSafeInteger(startup.cpuCount) || startup.cpuCount < 0) throw new Error('Invalid captured query CPU count.');
  env[CPU_METADATA] = JSON.stringify({ protocol: 1, cpuCount: startup.cpuCount });
  return env;
}

/** Qualified children use only transported CPU and scrub the actual descendant env. */
export function captureMcpQueryStartup(env: NodeJS.ProcessEnv, lazyCpu: () => number,
  liveBootstrapIpc: boolean, refusalRoot: string, nativeBootstrapIpc = liveBootstrapIpc): QueryStartupRequest {
  const qualified = env.CODEGRAPH_DAEMON_INTERNAL === '1' && nativeBootstrapIpc;
  const rawMetadata = env[CPU_METADATA];
  delete env[CPU_METADATA];
  if (!qualified) return captureQueryStartup(env, lazyCpu());

  const raw: NodeJS.ProcessEnv = {};
  for (const name of CONTROLS) {
    if (env[name] !== undefined) raw[name] = env[name];
    delete env[name];
  }
  try {
    if (!liveBootstrapIpc) throw new Error('Native candidate bootstrap IPC disconnected before query capture.');
    const value: unknown = rawMetadata === undefined ? null : JSON.parse(rawMetadata);
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Missing or malformed candidate CPU metadata.');
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record);
    if (keys.length !== 2 || !keys.includes('protocol') || !keys.includes('cpuCount')
      || record.protocol !== 1 || typeof record.cpuCount !== 'number'
      || !Number.isSafeInteger(record.cpuCount) || record.cpuCount < 0) {
      throw new Error('Unknown candidate CPU metadata.');
    }
    return captureQueryStartup(raw, record.cpuCount);
  } catch (error) {
    throw new QueryPolicyRefusalError('unknown', refusalRoot,
      'Inspect the candidate startup transport before retrying.',
      `Candidate query startup could not be proved: ${error instanceof Error ? error.message : String(error)}`);
  }
}
