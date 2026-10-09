// Type declarations for dsh-subagent-serializer.
// The runtime entry is lib/index.js (ESM); this file only documents the public
// surface for editors/consumers. It is not imported at runtime.

export interface SerializerConfig {
  /** ms to wait before a delegated call actually runs (default 30000) */
  delayMs: number;
  /** START tools to serialize (default ["subagent","subagent_fork","team_delegate"]) */
  toolNames: string[];
  /** WAKE tools (default ["send_message"]) */
  wakeToolNames: string[];
  /** INTERRUPT tools, pass-through (default ["interrupt_agent"]) */
  interruptToolNames: string[];
  /** max simultaneously running subagents; clamped down to engine total_slots (default 1) */
  maxConcurrency: number;
  /** launch watchdog: unconfirmed START slot held this long with calls blocked behind it is released (default 120000) */
  watchdogMs: number;
  /** ms a queued call waits before proceeding anyway (default 600000) */
  waitTimeoutMs: number;
  /** engine base URL for probing; "" disables probing (default "http://127.0.0.1:8080") */
  engineUrl: string;
  /** wait until the engine reports no active slot before START/WAKE proceeds (default true) */
  waitForEngineIdle: boolean;
  /** polling interval while waiting for idle (default 2000) */
  enginePollMs: number;
  /** per-request timeout for /props and /slots (default 3000) */
  probeTimeoutMs: number;
  /** total time to wait for the engine to go idle before proceeding anyway (default 600000) */
  idleTimeoutMs: number;
  /** how long a WAKE reservation is kept after delivery if the woken child never starts (default 15000) */
  wakeHoldMs: number;
  /** a running slot with no end for this long is force-vacated when calls are blocked (default 600000) */
  runningStaleMs: number;
}

export const name: string;
export const inject: readonly string[];
export const Config: unknown;
export function apply(ctx: unknown, config: SerializerConfig): void;
declare const _default: { name: string; inject: readonly string[]; Config: unknown; apply: typeof apply };
export default _default;
