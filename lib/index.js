// dsh-subagent-serializer
// ---------------------------------------------------------------------------
// DeepSeek Harness host plugin: serialize subagent starts behind a configurable
// delay, for single-concurrency local inference engines.
//
// Why this exists
//   A local inference engine that only serves one request at a time stalls when
//   the main agent and a subagent both generate concurrently: it must swap
//   contexts back and forth, which is extremely slow. This plugin makes the
//   subagent start later (a configurable delay) AND guarantees at most N
//   subagents are running at any moment (a gate), so the engine only ever sees
//   one request.
//
// How it works (all verified against DSH 0.2.0-rc.2 and re-verified against
// 0.2.1-rc.2 sources)
//   - DSH 0.2.1 changed delegation to "the tool returns at activation": the
//     `subagent` / `subagent_fork` tools hand back a child id as soon as the
//     child accepts the initial message, and the run continues in the
//     background until it settles. That does NOT change this plugin's model:
//     a gate slot still spans `subagent/start` (emitted at activation) to
//     `subagent/end` (emitted at settlement), so the slot tracks the real run,
//     not the tool call. `tools/execute` remains the one waterfall every tool
//     call passes through (dsh-tools dispatchScheduledExecution).
//   - dsh-tools runs every tool call through the `tools/execute` waterfall
//     (a cordis waterfall: each listener receives (exec, next) and must call
//     next() to continue the chain). We register a listener there.
//   - Tool families:
//       START tools (subagent / subagent_fork / team_delegate):
//           acquire a gate slot (queue when the cap is reached),
//           sleep(delayMs), wait for the engine idle, then call next(). The
//           slot is held until the child's subagent/end.
//       WAKE tools (send_message) from the MAIN agent:
//           acquire a slot (reservation), sleep(delayMs), wait for engine idle,
//           deliver, then keep the reservation until the woken child's end or a
//           per-reservation wakeHoldMs grace. Prevents a woken continuable
//           child's start (orphan) from overflowing capacity next to a START.
//       SUBAGENT-origin calls (any of the above, issued by a child):
//           skip the gate slot and the delay, but KEEP the engine-idle wait.
//           A child already occupies its own running slot; taking another gate
//           slot would make it wait for its own slot to free (self-block /
//           deadlock until timeout). The idle check is a pure read (no slot,
//           no self-wait), so it is kept to keep grandchildren / coordinator
//           delegations out of the engine's current generation. This covers
//           both a child messaging its parent (WAKE) and a child spawning a
//           grandchild (START / team_delegate coordinator).
//       INTERRUPT tools (interrupt_agent):
//           pass straight through — stopping must act immediately; the
//           subagent/end it causes is absorbed by the gate and frees the slot.
//   - FOREGROUND (`foreground: true`, the default): DSH 0.2.1 made delegation
//     strictly background — `subagent` / `subagent_fork` return the child id as
//     soon as the activation is accepted and the CALLING agent keeps generating
//     while the child runs. A gate alone therefore no longer keeps a
//     single-concurrency engine serial: the parent and the child overlap.
//     This plugin restores foreground semantics: the `tools/execute` listener
//     keeps the tool call OPEN until the child stops working, so the caller
//     cannot issue its next model request while the child generates.
//       - Tool arguments CANNOT be rewritten: the host deep-freezes
//         `exec.arguments` before this waterfall (dsh-tools `createExecution`),
//         so a plugin cannot force another tool's foreground flag. Foreground is
//         therefore produced here and nowhere else.
//       - Release signals (two, independent):
//           (a) a NEW `subagent/end` for that child id — exact, ~0 ms;
//           (b) two consecutive samples of "agent idle AND inbox empty" — the
//               missed-event fallback (also covers external backends and a
//               child that finished before the wait started).
//         `subagents.waitForChildren(parent)` is deliberately NOT used as a
//         release signal: 0.2.1's loop `continue`s on a child that is idle with
//         a pending inbox, so it can resolve before the child has even started.
//         "idle" alone is not finished either — DSH parks idle children with
//         queued work, which is why the inbox is part of the predicate.
//       - The wait is fail-open: bounded by foregroundTimeoutMs, honours the
//         tool call's abort signal, logs which mechanism released it, and never
//         turns a plugin bug into a hung delegation.
//   - @deepseek-ai/dsh-subagent emits scoped `subagent/start` and
//     `subagent/end` lifecycle events. A listener registered on an untagged
//     (global) context receives them all (dsh-scope: untagged contexts match
//     every carrier). `subagent/end` releases a gate slot.
//   - Engine probe (optional, fail-open): we ask the engine how many concurrent
//     requests it really supports (`/props` -> total_slots) and clamp the gate
//     capacity to it, and we wait until the engine reports idle (`/slots` ->
//     every slot is_processing !== true) right before starting, so a child is
//     never dropped into the engine's FIFO queue mid-generation. The probe uses
//     the Node built-in `fetch` (NOT ctx.web, which refuses loopback/private
//     addresses). A circuit breaker disables idle-waiting in this process after
//     repeated probe failures (fail-open).
//   - Watchdogs (all fail-open, never wedge the queue):
//       launch watchdog  - an unconfirmed START slot is force-released ONLY when
//                          a call is blocked behind it, and the hold is measured
//                          from the SPAWN call (after the delay + engine-idle
//                          wait), so legitimate pre-start waiting is never
//                          interrupted (a slow-starting child on a busy engine
//                          is otherwise kept, not mis-killed);
//       wake grace      - each WAKE reservation carries its OWN timer, armed
//                          right before delivery; if the woken child never
//                          starts, the reservation is released after wakeHoldMs
//                          (per-entry, so a second WAKE cannot disarm or take
//                          over the first's grace);
//       running stale   - a running slot with no subagent/end for runningStaleMs
//                          is force-vacated when calls are blocked.
//   - Queue timeouts (waitTimeoutMs) let any queued call proceed anyway
//     (fail-open), so a missing event can never hang a call forever.
//   - Failure is fail-open: plugin-internal errors are logged and the tool call
//     still proceeds; only the tool's own error is rethrown.
//
// Known limitation (event attribution)
//   subagent/start and subagent/end carry no session/run id we can correlate
//   with a specific acquired slot, so the gate counts anonymously (FIFO). In
//   the rare windows where a watchdog intervenes, a late start may be counted
//   against the successor's slot and `#running` may briefly exceed capacity;
//   the engine-idle gate remains the real concurrency guard, and the count
//   self-heals on the next end. Do not rely on the gate as a hard lock under
//   watchdog intervention.
//
// Configuration (plugin Config, editable in the Harness plugin settings):
//   delayMs           - ms to wait before a delegated call actually runs
//                       (default 30000)
//   toolNames         - START tools to serialize (default
//                       ["subagent","subagent_fork","team_delegate"]; team_roles/
//                       team_find are pure queries and are deliberately excluded;
//                       "workflow" is excluded by design)
//   wakeToolNames     - WAKE tools (default ["send_message"])
//   interruptToolNames- INTERRUPT tools, pass-through (default ["interrupt_agent"])
//   maxConcurrency    - max simultaneously running subagents (default 1);
//                       clamped down to the engine's total_slots when probed
//   watchdogMs        - launch watchdog: unconfirmed START slot held this long
//                       with calls blocked behind it is released (default 120000)
//   waitTimeoutMs     - ms a QUEUED call waits before proceeding anyway (default 600000)
//   engineUrl         - engine base URL for probing (default "http://127.0.0.1:8080";
//                       "" disables probing entirely — recommended when no engine
//                       probe is wanted, to avoid per-call idle checks)
//   waitForEngineIdle - wait until the engine reports no active slot before
//                       START/WAKE proceeds (default true)
//   enginePollMs      - polling interval while waiting for idle (default 2000)
//   probeTimeoutMs    - per-request timeout for /props and /slots (default 3000)
//   idleTimeoutMs     - total time to wait for the engine to go idle before
//                       proceeding anyway (default 600000)
//   wakeHoldMs        - per-reservation grace for a WAKE slot whose woken child
//                       never starts (default 15000)
//   runningStaleMs    - a running slot with no end for this long is force-vacated
//                       when calls are blocked (default 600000)
//   foreground        - keep a START tool call open until its child settles, so
//                       the caller does not generate next to the child (default
//                       true — this is the 0.2.1 background-delegation fix)
//   foregroundTimeoutMs - safety cap on a foreground wait; the call is released
//                       (fail-open) after this long (default 1800000)
//   foregroundWakes   - also block `send_message` until its target child stops
//                       working (id-keyed off `agent_id`; default true — a wake
//                       otherwise lets the caller generate next to the woken
//                       child, which is the same overlap this plugin exists to
//                       remove; set false to keep wakes fire-and-forget)
// ---------------------------------------------------------------------------

import z from "@deepseek-ai/schemastery";

export const name = "dsh-subagent-serializer";

export const inject = [];

export const Config = z.object({
  delayMs: z.number().min(0).max(Number.MAX_SAFE_INTEGER).default(30000),
  toolNames: z.array(z.string().min(1)).default(["subagent", "subagent_fork", "team_delegate"]),
  wakeToolNames: z.array(z.string().min(1)).default(["send_message"]),
  interruptToolNames: z.array(z.string().min(1)).default(["interrupt_agent"]),
  maxConcurrency: z.natural().min(1).max(1024).default(1),
  watchdogMs: z.number().min(1000).max(Number.MAX_SAFE_INTEGER).default(120000),
  waitTimeoutMs: z.number().min(1000).max(Number.MAX_SAFE_INTEGER).default(600000),
  engineUrl: z.string().default("http://127.0.0.1:8080"),
  waitForEngineIdle: z.boolean().default(true),
  enginePollMs: z.number().min(200).max(Number.MAX_SAFE_INTEGER).default(2000),
  probeTimeoutMs: z.number().min(500).max(Number.MAX_SAFE_INTEGER).default(3000),
  idleTimeoutMs: z.number().min(1000).max(Number.MAX_SAFE_INTEGER).default(600000),
  wakeHoldMs: z.number().min(1000).max(Number.MAX_SAFE_INTEGER).default(15000),
  runningStaleMs: z.number().min(1000).max(Number.MAX_SAFE_INTEGER).default(600000),
  foreground: z.boolean().default(true),
  foregroundTimeoutMs: z.number().min(1000).max(Number.MAX_SAFE_INTEGER).default(1800000),
  foregroundWakes: z.boolean().default(true),
});

/** Sleep that aborts early on the caller's signal and removes its listener on
 * BOTH completion paths (normal timeout and abort), so long-lived signals do
 * not accumulate listeners. */
function sleep(ms, signal) {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    if (signal !== undefined && signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(cleanupAndResolve, ms);
    if (typeof timer.unref === "function") timer.unref();
    let settled = false;
    function cleanupAndResolve() {
      if (settled) return;
      settled = true;
      if (signal !== undefined) signal.removeEventListener("abort", onAbort);
      resolve();
    }
    function onAbort() {
      clearTimeout(timer);
      cleanupAndResolve();
    }
    if (signal !== undefined) {
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

/** GET a JSON endpoint with a timeout. Returns parsed JSON or throws. */
async function getJson(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: { accept: "application/json" },
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

/** Probe the engine once: read /props.total_slots. Returns the slot count or
 * undefined when the probe fails (caller degrades). Never throws. */
async function probeEngineSlots(engineUrl, timeoutMs, onLog) {
  try {
    const base = engineUrl.replace(/\/+$/, "");
    const props = await getJson(`${base}/props`, timeoutMs);
    const slots = Number(props && props.total_slots);
    if (Number.isFinite(slots) && slots >= 1) return slots;
    onLog?.(
      `engine probe: /props did not report a positive total_slots (got ${JSON.stringify(props && props.total_slots)}); keeping configured maxConcurrency`,
    );
    return undefined;
  } catch (error) {
    onLog?.(`engine probe failed (${String(error && error.message ? error.message : error)}); using configured delays only (fail-open)`);
    return undefined;
  }
}

/**
 * Wait until every engine slot reports is_processing !== true (or the slots
 * list is empty). Returns true when idle/fail-open, false on timeout. Never
 * throws. A single transient probe failure does NOT immediately fail open —
 * only CONSECUTIVE_FAILS_TO_FAIL_OPEN failures in a row do, and when that
 * happens `onFailOpen` is invoked so the caller can trip the circuit breaker.
 */
async function waitForEngineIdle(engineUrl, pollMs, timeoutMs, probeTimeoutMs, signal, onLog, onFailOpen) {
  const base = engineUrl.replace(/\/+$/, "");
  const deadlineAt = Date.now() + timeoutMs;
  let loggedWait = false;
  let consecutiveFails = 0;
  const CONSECUTIVE_FAILS_TO_FAIL_OPEN = 3;
  for (;;) {
    if (signal !== undefined && signal.aborted) return false;
    try {
      const slots = await getJson(`${base}/slots`, probeTimeoutMs);
      consecutiveFails = 0;
      if (Array.isArray(slots) && slots.every((s) => !s || s.is_processing !== true)) {
        return true; // empty list or all idle -> idle
      }
    } catch (error) {
      consecutiveFails += 1;
      if (consecutiveFails >= CONSECUTIVE_FAILS_TO_FAIL_OPEN) {
        onLog?.(`engine idle probe failed ${consecutiveFails}x (${String(error && error.message ? error.message : error)}); proceeding (fail-open)`);
        onFailOpen?.();
        return true;
      }
    }
    if (Date.now() >= deadlineAt) {
      onLog?.(`engine did not report idle within ${timeoutMs}ms; proceeding anyway (fail-open)`);
      return false;
    }
    if (!loggedWait) {
      loggedWait = true;
      onLog?.("engine is busy; waiting for it to report idle before proceeding");
    }
    await sleep(pollMs, signal);
  }
}

/**
 * A counting gate: at most `capacity` holders are admitted at once; the rest
 * queue. Unconfirmed holders are tracked as a FIFO of entries (`#entries`),
 * each tagged 'start' or 'wake' with its own acquisition timestamp and — for
 * WAKE — its own grace timer. `confirmRunning` (subagent/start observer) moves
 * the OLDEST entry to running; `vacate` (subagent/end observer) releases one
 * running slot. Watchdogs are fail-open and never wedge the queue. Every
 * mutating method is idempotent and safe under fail-open.
 */
class SubagentGate {
  #capacity;
  #running = 0; // holders confirmed by subagent/start (incl. orphan woken children)
  #entries = []; // unconfirmed holders: { kind: 'start'|'wake', at, timer? }
  #waiters = []; // { wait, cancelled, kind }
  #watchdogMs;
  #wakeHoldMs;
  #runningStaleMs;
  #onWarn;
  #launchTimer = undefined;
  #staleTimer = undefined;
  #disposed = false;

  constructor(capacity, watchdogMs, wakeHoldMs, runningStaleMs, onWarn) {
    this.#capacity = capacity;
    this.#watchdogMs = watchdogMs;
    this.#wakeHoldMs = wakeHoldMs;
    this.#runningStaleMs = runningStaleMs;
    this.#onWarn = onWarn;
  }

  get active() {
    return this.#entries.length + this.#running;
  }

  /** Clamp capacity to the engine's real concurrency (from the probe). Only
   * ever lowers it; never raises it past the configured value. */
  setCapacity(capacity) {
    if (capacity >= 1 && capacity < this.#capacity) {
      this.#capacity = capacity;
      this.#pump();
    }
  }

  /** Try to admit immediately; if at capacity, return a waiter handle whose
   * `promise` resolves when a slot frees up and whose `cancel()` removes the
   * queued request (fail-open path). `kind` is 'start' or 'wake'. When the gate
   * is disposed (plugin stop/HMR), admit immediately (pass-through) so in-flight
   * hooks are not wedged. */
  acquire(kind) {
    if (this.#disposed) return undefined; // pass-through after plugin stop
    if (this.#entries.length + this.#running < this.#capacity) {
      this.#armEntry(kind);
      return undefined;
    }
    let resolve;
    const promise = new Promise((res) => {
      resolve = res;
    });
    const entry = { wait: promise, cancelled: false, kind };
    entry.resolve = () => {
      if (!entry.cancelled) resolve();
    };
    entry.cancel = () => {
      entry.cancelled = true;
      const idx = this.#waiters.indexOf(entry);
      if (idx >= 0) this.#waiters.splice(idx, 1); // drop it, don't accumulate
    };
    this.#waiters.push(entry);
    return { promise, cancel: () => entry.cancel() };
  }

  /** Push an unconfirmed holder. WAKE grace is NOT armed here — it is armed
   * right before delivery via {@link armWakeGrace}, so the hold counts only
   * from after the message is delivered (per the review's R1 fix). */
  #armEntry(kind) {
    // phase: 'gating' = acquired, still in the delay / engine-idle wait
    // (legitimate holding); 'spawning' = the hook is about to call next().
    // The launch watchdog only releases 'spawning' holders, so a queued caller
    // can never steal the slot while the holder is legitimately waiting.
    const entry = { kind, at: Date.now(), phase: "gating" };
    this.#entries.push(entry);
    this.#armLaunch();
  }

  /** Arm the WAKE grace timer for the OLDEST wake entry that has no grace
   * timer yet. Called right before next() (after delay + engine idle wait), so
   * the wakeHoldMs hold starts only once the message is actually delivered.
   * The `timer === undefined` filter makes each concurrent WAKE hook arm its
   * OWN entry: the second hook skips the already-armed first entry instead of
   * re-arming it and leaving its own entry with no grace at all. */
  armWakeGrace() {
    const entry = this.#entries.find((e) => e.kind === "wake" && e.timer === undefined);
    if (entry === undefined || this.#wakeHoldMs <= 0) return;
    if (entry.timer !== undefined) clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      entry.timer = undefined;
      const idx = this.#entries.indexOf(entry);
      if (idx >= 0) {
        this.#onWarn?.(`WAKE reservation unconfirmed after ${this.#wakeHoldMs}ms; releasing it (fail-open)`);
        this.#entries.splice(idx, 1);
        this.#pump();
      }
    }, this.#wakeHoldMs);
  }

  /** Mark the OLDEST still-gating START entry as spawning (its hook is about
   * to call next()). The launch watchdog measures its hold from this moment,
   * so the delay + engine-idle wait is never mis-read as a lost start.
   * Anonymous-FIFO caveat: with capacity>1 and uneven hook progress the marked
   * entry may belong to another hook (fail-open bounded); with the default
   * capacity of 1 (and the engine probe clamping to total_slots) it is exact. */
  markSpawning() {
    const entry = this.#entries.find((e) => e.kind === "start" && e.phase === "gating");
    if (entry === undefined) return;
    entry.phase = "spawning";
    entry.spawnAt = Date.now();
  }

  /** Confirm that a child started: the OLDEST unconfirmed holder becomes
   * running (FIFO), or an orphan is counted when no holder exists. */
  confirmRunning() {
    if (this.#entries.length > 0) {
      const entry = this.#entries.shift();
      if (entry.timer !== undefined) clearTimeout(entry.timer);
      this.#running += 1;
    } else {
      // Orphan start (a woken child, or a late start after a watchdog released
      // its holder). The engine-idle gate is the real concurrency guard;
      // counting it keeps the counter honest.
      this.#running += 1;
      if (this.#running > this.#capacity) {
        this.#onWarn?.(`orphan subagent/start pushed running to ${this.#running} (capacity ${this.#capacity}); engine-idle gate remains the concurrency guard`);
      }
    }
    this.#armStale();
  }

  /** Release one running slot (called on subagent/end). */
  vacate() {
    if (this.#running > 0) {
      this.#running -= 1;
    } else if (this.#entries.length > 0) {
      // Ended without a start confirmation (unusual but safe) — releases the
      // OLDEST held (unconfirmed) reservation.
      const entry = this.#entries.shift();
      if (entry.timer !== undefined) clearTimeout(entry.timer);
    } else {
      return;
    }
    if (this.#running === 0) this.#disarmStale();
    this.#pump();
  }

  /** Force-release the OLDEST unconfirmed holder (watchdog / fail path). */
  releaseUnconfirmed() {
    if (this.#entries.length > 0) {
      const entry = this.#entries.shift();
      if (entry.timer !== undefined) clearTimeout(entry.timer);
      this.#pump();
      return true;
    }
    return false;
  }

  /**
   * Launch watchdog: it only force-releases an unconfirmed START slot when a
   * call is blocked behind it (queue wedged) AND the slot has been SPAWNING
   * (marked via markSpawning) for at least `watchdogMs`. The delay + engine
   * idle wait before the spawn is legitimate holding and is never interrupted.
   * A slow-starting child with nobody waiting is kept and re-armed — a busy
   * engine can delay a background job's subagent/start by minutes, and
   * mis-killing it would over-serialize.
   */
  #armLaunch() {
    this.#disarmLaunch();
    if (this.#watchdogMs <= 0) return;
    this.#launchTimer = setTimeout(() => {
      this.#launchTimer = undefined;
      if (this.#entries.length > 0) {
        const oldest = this.#entries[0];
        if (this.#waiters.length > 0
          && oldest.kind === "start"
          && oldest.phase === "spawning"
          && oldest.spawnAt !== undefined
          && Date.now() - oldest.spawnAt >= this.#watchdogMs) {
          this.#onWarn?.(`unconfirmed START slot held ${this.#watchdogMs}ms and a call is blocked; force-releasing it (fail-open)`);
          this.releaseUnconfirmed();
        }
        if (this.#entries.length > 0) this.#armLaunch();
      }
    }, this.#watchdogMs);
  }

  #disarmLaunch() {
    if (this.#launchTimer !== undefined) {
      clearTimeout(this.#launchTimer);
      this.#launchTimer = undefined;
    }
  }

  /** Running stale watch: if a running slot sees no subagent/end for
   * `runningStaleMs` AND calls are blocked behind it, force-vacate one (the
   * engine-idle gate keeps real concurrency safe). Re-arms while running>0. */
  #armStale() {
    if (this.#runningStaleMs <= 0 || this.#running === 0 || this.#staleTimer !== undefined) return;
    this.#staleTimer = setTimeout(() => {
      this.#staleTimer = undefined;
      if (this.#running > 0) {
        if (this.#waiters.length > 0) {
          this.#onWarn?.(`running slot has seen no subagent/end for ${this.#runningStaleMs}ms and a call is blocked; force-releasing it (fail-open)`);
          this.#running -= 1;
          this.#pump();
        }
        if (this.#running > 0) this.#armStale();
      }
    }, this.#runningStaleMs);
  }

  #disarmStale() {
    if (this.#staleTimer !== undefined) {
      clearTimeout(this.#staleTimer);
      this.#staleTimer = undefined;
    }
  }

  /** Admit queued holders whenever capacity allows. */
  #pump() {
    if (this.#disposed) return;
    while (this.#waiters.length > 0 && this.#entries.length + this.#running < this.#capacity) {
      const entry = this.#waiters.shift();
      if (entry.cancelled) continue; // fail-open bypass already ran; never admit it
      this.#armEntry(entry.kind);
      entry.resolve();
    }
  }

  /** Clear all timers, release queued waiters (fail-open), and refuse further
   * admission (plugin stop / HMR). In-flight hooks continue to call next();
   * queued waiters resolve immediately so they do not hang until waitTimeoutMs.
   * New acquire() calls admit immediately (gate empty, pass-through). */
  dispose() {
    this.#disposed = true;
    this.#disarmLaunch();
    this.#disarmStale();
    for (const entry of this.#entries) {
      if (entry.timer !== undefined) clearTimeout(entry.timer);
    }
    this.#entries = [];
    const waiters = this.#waiters;
    this.#waiters = [];
    for (const entry of waiters) {
      entry.resolve(); // fail-open: let queued calls proceed rather than hang
    }
  }
}

/** Whether the calling agent is itself a subagent (child issuing a call). */
function isSubagentCaller(exec) {
  try {
    return !!(exec && exec.agent && exec.agent.session && exec.agent.session.header
      && exec.agent.session.header.origin === "subagent");
  } catch {
    return false;
  }
}

/**
 * Read a host service without declaring `inject`. Delegation only needs the
 * service lazily (inside a tool call), and an unloaded service must degrade to
 * "no second signal", never to a plugin-load failure.
 */
function serviceOf(ctx, name) {
  try {
    const get = typeof ctx.get === "function" ? ctx.get.bind(ctx) : undefined;
    const service = get === undefined ? undefined : get(name);
    if (service !== undefined) return service;
    return ctx[name];
  } catch {
    return undefined;
  }
}

/** Child id a delegation tool returned in its structured value, if any.
 * `subagent`/`subagent_fork` answer `{ kind: "activation", subagentId }`;
 * `team_delegate` answers `{ kind: "continuable", subagentId }` in background
 * mode and `{ kind: "foreground", ... }` (already settled) in foreground mode.
 */
function activationChildId(result) {
  try {
    if (result === undefined || result === null || result.isError === true) return undefined;
    const value = result.value;
    if (value === undefined || value === null || typeof value !== "object") return undefined;
    if (value.kind !== undefined && value.kind !== "activation" && value.kind !== "continuable") return undefined;
    return typeof value.subagentId === "string" && value.subagentId.length > 0 ? value.subagentId : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Target child id of a wake tool call. `send_message` takes `agent_id`
 * (`@deepseek-ai/dsh-tool-subagent-control`); the arguments object is frozen by
 * the host but readable. Unknown shapes simply yield undefined (no blocking).
 */
function wakeTargetOf(exec) {
  try {
    const args = exec === undefined || exec === null ? undefined : exec.arguments;
    if (args === undefined || args === null || typeof args !== "object") return undefined;
    for (const key of ["agent_id", "agentId", "child_id", "childId", "target_id", "targetId"]) {
      const value = args[key];
      if (typeof value === "string" && value.length > 0) return value;
    }
    return undefined;
  } catch {
    return undefined;
  }
}

/**
 * The entry point. `apply(ctx, config)` is the cordis plugin contract.
 * Everything here is effect-scoped (ctx.on registers with the fiber and is
 * cleaned up on plugin stop / HMR; gate.dispose() runs via ctx.effect).
 */
export function apply(ctx, config) {
  const log = (level, msg) => ctx.logger[level](`dsh-subagent-serializer: ${msg}`);
  const engineUrl = String(config.engineUrl || "").trim();
  const probeEnabled = engineUrl.length > 0;

  const gate = new SubagentGate(
    config.maxConcurrency,
    config.watchdogMs,
    config.wakeHoldMs,
    config.runningStaleMs,
    (msg) => log("warn", msg),
  );
  ctx.effect(() => () => gate.dispose());
  const startTools = new Set(config.toolNames);
  const wakeTools = new Set(config.wakeToolNames);
  const interruptTools = new Set(config.interruptToolNames);

  // Circuit breaker: once the engine is repeatedly unreachable, stop doing
  // per-call idle checks in this process (fail-open to fixed delay only).
  let idleWaitDisabled = false;

  // Foreground bookkeeping. A waiter needs "did this child emit a NEW
  // subagent/end after I started waiting?", which a Set cannot answer: a child
  // that already settled must not release a later wait (that is what a wake
  // does), so settlement is counted per child id.
  // `foregroundCountersMax` bounds the map; an evicted id reports Infinity so a
  // waiter that outlived the eviction releases instead of hanging to timeout.
  const settleCounts = new Map();
  const settleOrder = [];
  const foregroundCountersMax = 512;

  function settlementCount(childId) {
    const count = settleCounts.get(childId);
    return count === undefined ? 0 : count;
  }

  function noteSettled(childId) {
    settleCounts.set(childId, settlementCount(childId) + 1);
    settleOrder.push(childId);
    while (settleOrder.length > foregroundCountersMax) {
      const evicted = settleOrder.shift();
      if (evicted !== undefined) settleCounts.set(evicted, Number.POSITIVE_INFINITY);
    }
  }

  // Lifecycle observers. Scoped events dispatched with a scope carrier still
  // reach this global (untagged) context: dsh-scope admits untagged contexts
  // for every carrier.
  ctx.on("subagent/start", () => {
    gate.confirmRunning();
  });

  ctx.on("subagent/end", (info) => {
    gate.vacate();
    const childId = info !== undefined && info !== null && typeof info.id === "string" ? info.id : undefined;
    if (childId !== undefined) noteSettled(childId);
  });

  // Asynchronous engine probe (fail-open, non-blocking): confirm the engine's
  // real concurrency and clamp the gate capacity down to it. Runs once at load;
  // an engine that (re)starts later with a different total_slots is not
  // re-probed (see README). A probe failure here does NOT trip the circuit
  // breaker: /props may be missing while /slots (used by the idle check) works
  // fine, so we keep idle checks enabled and only log (R4 fix).
  if (probeEnabled) {
    (async () => {
      const slots = await probeEngineSlots(engineUrl, config.probeTimeoutMs, (msg) => log("warn", msg));
      if (slots !== undefined && slots < config.maxConcurrency) {
        gate.setCapacity(slots);
        log("info", `engine reports total_slots=${slots}; maxConcurrency clamped from ${config.maxConcurrency} to ${slots}`);
      } else if (slots !== undefined) {
        log("info", `engine probe: total_slots=${slots}; gate capacity stays at ${config.maxConcurrency}`);
      }
    })();
  }

  /** Wait for the engine to go idle, honoring the circuit breaker. Fail-open. */
  async function waitForEngineIdleChecked(exec) {
    if (!probeEnabled || !config.waitForEngineIdle || idleWaitDisabled) return;
    await waitForEngineIdle(
      engineUrl,
      config.enginePollMs,
      config.idleTimeoutMs,
      config.probeTimeoutMs,
      exec?.signal,
      (msg) => log("info", msg),
      () => { idleWaitDisabled = true; log("warn", "engine idle probing failing repeatedly; disabling idle waits in this process (fail-open)"); },
    );
  }

  /**
   * Whether the child is currently doing work.
   *  true  — a live agent that is running, or one that is idle with a queued
   *          turn/step (DSH parks idle children with pending inbox entries;
   *          "idle" alone does NOT mean finished);
   *  false — a live agent that is idle with an empty inbox (finished work, or
   *          not started yet — callers confirm over consecutive samples);
   *  undefined — no such agent (unknown id / external backend).
   * `status` is deliberately not the only input: 0.2.1's waitForChildren skips
   * exactly the "idle but parked with a pending inbox" case, so a guard built
   * on `status !== "idle"` would agree with that skip and release early.
   */
  function childBusy(childId) {
    const agents = serviceOf(ctx, "agents");
    const agent = agents !== undefined && typeof agents.get === "function" ? agents.get(childId) : undefined;
    if (agent === undefined || agent === null) return undefined;
    if (agent.status !== "idle") return true;
    const inbox = agent.inbox;
    if (inbox === undefined || inbox === null) return false;
    const pending = (queue) => {
      if (Array.isArray(queue)) return queue.length > 0;
      return typeof queue === "number" && queue > 0;
    };
    return pending(inbox.nextTurn) || pending(inbox.nextStep);
  }

  /**
   * Block the caller until the child stops working (foreground semantics).
   *
   * Two independent release signals, deliberately NOT including
   * `subagents.waitForChildren`: its loop `continue`s on a child that is idle
   * with a pending inbox, so it can resolve while the child has not started —
   * using it as a release would silently restore background behaviour.
   *
   *   1. a NEW `subagent/end` for this id (event, exact, ~0ms),
   *   2. two consecutive samples of "idle and inbox empty" (covers a missed
   *      event, an external backend, and a child that finished before we
   *      registered; costs <= 2 x pollIntervalMs),
   *   3. the safety valve (`foregroundTimeoutMs`) and the call's abort signal.
   */
  async function awaitChildSettled(exec, childId, seenCount, label) {
    const startedAt = Date.now();
    const deadline = startedAt + config.foregroundTimeoutMs;
    const pollMs = Math.max(200, Math.min(1000, Math.floor(config.enginePollMs / 2) || 500));
    let quiet = 0;
    let lastReason = "subagent/end";

    for (;;) {
      if (settlementCount(childId) > seenCount) {
        log("info", `${label}: subagent ${childId} settled after ${Date.now() - startedAt}ms (released by subagent/end)`);
        return "subagent/end";
      }
      const busy = childBusy(childId);
      if (busy === false) {
        quiet += 1;
        if (quiet >= 2) {
          log("info", `${label}: subagent ${childId} idle for ${pollMs * quiet}ms without a new subagent/end (finished, or the event was missed); releasing after ${Date.now() - startedAt}ms`);
          return "idle";
        }
      } else {
        quiet = 0;
      }
      if (exec?.signal !== undefined && exec.signal.aborted) {
        log("info", `${label}: call aborted after ${Date.now() - startedAt}ms while waiting for subagent ${childId}`);
        return "aborted";
      }
      if (Date.now() >= deadline) {
        log("warn", `${label}: subagent ${childId} did not settle within ${config.foregroundTimeoutMs}ms; releasing the call (fail-open)`);
        lastReason = "timeout";
        return lastReason;
      }
      await sleep(pollMs, exec?.signal);
    }
  }

  // Serialize + delay the delegation tools.
  ctx.on("tools/execute", async (exec, next) => {
    const name = exec === undefined ? undefined : exec.name;
    if (name === undefined) return next();

    if (interruptTools.has(name)) {
      // Interrupt is a stop operation: act immediately. Waiting for idle would
      // deadlock (the target is running by definition); the subagent/end it
      // causes is absorbed by the gate and frees capacity.
      return next();
    }

    const isStart = startTools.has(name);
    const isWake = wakeTools.has(name);
    if (!isStart && !isWake) return next();

    if (isSubagentCaller(exec)) {
      // A call issued BY a subagent is part of that child's own run; it already
      // occupies its own running slot, so taking another gate slot would make it
      // wait for its own slot to free (self-block / deadlock until timeout).
      // Skip acquire + delay, but KEEP the engine-idle wait (R5): a grandchild
      // or coordinator delegation should still not collide with the engine's
      // current generation, and the idle check is a pure read (no slot, no
      // self-wait).
      try {
        await waitForEngineIdleChecked(exec);
      } catch (error) {
        log("warn", `${name}: subagent-origin engine idle check failed (${String(error && error.message ? error.message : error)}); proceeding (fail-open)`);
      }
      return next();
    }

    let holdingSlot = false;

    // Phase 1: gating (acquire / delay / engine idle / arm watchdogs). Any
    // plugin-internal error here is logged and we STILL call next() (fail-open);
    // we never break the delegation because of our own bookkeeping.
    try {
      const kind = isStart ? "start" : "wake";
      const handle = gate.acquire(kind);
      if (handle !== undefined) {
        log("info", `${name} queued (${gate.active} busy); waiting for a free slot`);
        const released = await raceTimeout(handle.promise, config.waitTimeoutMs, exec?.signal);
        if (released !== undefined) {
          handle.cancel();
          log(released === "aborted" ? "info" : "warn",
            released === "aborted"
              ? `${name}: call aborted while queued; not taking a slot`
              : `${name} waited more than ${config.waitTimeoutMs}ms for a slot; proceeding anyway (fail-open)`);
        } else {
          holdingSlot = true;
        }
      } else {
        holdingSlot = true;
      }

      if (config.delayMs > 0) {
        log("info", `${name} will run after ${config.delayMs}ms delay`);
        await sleep(config.delayMs, exec?.signal);
      }

      await waitForEngineIdleChecked(exec);

      if (isStart) {
        if (holdingSlot) {
          // The launch watchdog starts measuring HERE (right before the spawn),
          // not at acquire: the delay + engine-idle wait is legitimate holding,
          // and releasing during it would over-admit a queued caller (F1 fix).
          gate.markSpawning();
        }
      } else if (holdingSlot) {
        // WAKE grace starts here — right before delivery — so the reservation
        // is held through the delay + idle wait and only times out if the woken
        // child never starts (R1 fix). Only when we actually hold a slot: a
        // timed-out (fail-open) WAKE has no entry and must not arm someone
        // else's grace (F3 fix).
        gate.armWakeGrace();
      }
    } catch (error) {
      log("warn", `${name}: plugin gate error (${String(error && error.message ? error.message : error)}); proceeding (fail-open)`);
      if (holdingSlot) gate.releaseUnconfirmed();
      holdingSlot = false;
    }

    // Phase 2: the real tool call. Only the tool's own error is rethrown; on
    // failure we release any unconfirmed slot we hold so the queue cannot wedge.
    // NOTE: `exec.arguments` is deep-frozen by the host before this waterfall
    // (dsh-tools `createExecution`), so a plugin structurally cannot rewrite a
    // tool argument — e.g. forcing `run_in_background: false` is impossible.
    // Foreground therefore has to be produced here, in Phase 3.
    const wakeTarget = isWake ? wakeTargetOf(exec) : undefined;
    const wakeSeen = wakeTarget === undefined ? 0 : settlementCount(wakeTarget);

    let result;
    try {
      result = await next();
    } catch (error) {
      if (holdingSlot) {
        if (!gate.releaseUnconfirmed()) {
          log("info", `${name}: slot already confirmed; it will be released by subagent/end`);
        }
      }
      throw error;
    }

    // Phase 3 (foreground): hold the tool call open until the child settles.
    // DSH >= 0.2.1 returns the child id at activation and lets the caller keep
    // generating; without this phase the parent and the child overlap on a
    // single-concurrency engine. Every failure path here is fail-open: the tool
    // result is still returned, so a plugin bug can never hang a delegation.
    try {
      if (config.foreground && isStart) {
        const childId = activationChildId(result);
        if (childId === undefined) {
          log("info", `${name}: no child id in the tool result (the call already produced the child's outcome); no wait needed`);
        } else {
          log("info", `${name}: foreground — holding the call until subagent ${childId} settles`);
          await awaitChildSettled(exec, childId, settlementCount(childId), name);
        }
      } else if (config.foregroundWakes && isWake) {
        if (wakeTarget === undefined) {
          log("warn", `${name}: no target agent id in the call arguments; cannot block this wake`);
        } else {
          log("info", `${name}: foreground — holding the wake until agent ${wakeTarget} stops working`);
          await awaitChildSettled(exec, wakeTarget, wakeSeen, name);
        }
      }
    } catch (error) {
      log("warn", `${name}: foreground wait failed (${String(error && error.message ? error.message : error)}); returning the tool result (fail-open)`);
    }

    return result;
  });
}

/** Race a queued wait against its timeout AND the call's abort signal.
 * Resolves `undefined` when the wait itself settled, or the winning reason
 * ("timeout" / "aborted"). A cancelled turn must not sit out waitTimeoutMs: the
 * host drains started tool calls, so an un-cancellable queue wait freezes the
 * whole turn for up to that long. */
function raceTimeout(wait, timeoutMs, signal) {
  return new Promise((resolve) => {
    let settled = false;
    function onAbort() {
      done("aborted");
    }
    function done(reason) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (signal !== undefined) signal.removeEventListener("abort", onAbort);
      resolve(reason);
    }
    const timer = setTimeout(() => done("timeout"), Math.max(0, timeoutMs));
    if (typeof timer.unref === "function") timer.unref();
    if (signal !== undefined) {
      if (signal.aborted) {
        done("aborted");
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }
    wait.then(() => done(undefined));
  });
}

export default { name, inject, Config, apply };
