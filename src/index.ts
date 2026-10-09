// dsh-subagent-serializer — 类型化源码（维护参考）
// ---------------------------------------------------------------------------
// 本文件是 lib/index.js 的 TypeScript 版本，供阅读/维护/后续用 tsc 构建。
// 实际运行时加载的是 lib/index.js（ESM，无构建依赖）。
// 类型仅做文档用途；与 lib/index.js 的行为保持一致。
// ---------------------------------------------------------------------------

import z from "@deepseek-ai/schemastery";
import type { Context } from "@deepseek-ai/cordis";

export const name = "dsh-subagent-serializer";

export const inject: readonly string[] = [];

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
});

type ConfigType = z.infer<typeof Config>;

/** 睡眠：支持调用方取消信号，且正常完成与 abort 两条路径都移除监听（长寿命 signal 不累积）。 */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
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

/** GET 一个 JSON 端点，带超时。返回解析后的 JSON 或抛错。 */
async function getJson(url: string, timeoutMs: number): Promise<unknown> {
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

/** 探测引擎并发槽数（/props.total_slots）。失败返回 undefined（fail-open）。 */
async function probeEngineSlots(
  engineUrl: string,
  timeoutMs: number,
  onLog?: (msg: string) => void,
): Promise<number | undefined> {
  try {
    const base = engineUrl.replace(/\/+$/, "");
    const props = (await getJson(`${base}/props`, timeoutMs)) as { total_slots?: unknown };
    const slots = Number(props && props.total_slots);
    if (Number.isFinite(slots) && slots >= 1) return slots;
    onLog?.(
      `engine probe: /props did not report a positive total_slots (got ${JSON.stringify(props && props.total_slots)}); keeping configured maxConcurrency`,
    );
    return undefined;
  } catch (error) {
    onLog?.(`engine probe failed (${String((error as Error).message ?? error)}); using configured delays only (fail-open)`);
    return undefined;
  }
}

/**
 * 等待所有引擎槽位 is_processing !== true（空列表视为空闲）。连续失败
 * CONSECUTIVE_FAILS_TO_FAIL_OPEN 次才 fail-open，并触发 onFailOpen（熔断）。
 */
async function waitForEngineIdle(
  engineUrl: string,
  pollMs: number,
  timeoutMs: number,
  probeTimeoutMs: number,
  signal: AbortSignal | undefined,
  onLog?: (msg: string) => void,
  onFailOpen?: () => void,
): Promise<boolean> {
  const base = engineUrl.replace(/\/+$/, "");
  const deadlineAt = Date.now() + timeoutMs;
  let loggedWait = false;
  let consecutiveFails = 0;
  const CONSECUTIVE_FAILS_TO_FAIL_OPEN = 3;
  for (;;) {
    if (signal !== undefined && signal.aborted) return false;
    try {
      const slots = (await getJson(`${base}/slots`, probeTimeoutMs)) as { is_processing?: unknown }[];
      consecutiveFails = 0;
      if (Array.isArray(slots) && slots.every((s) => !s || s.is_processing !== true)) {
        return true;
      }
    } catch (error) {
      consecutiveFails += 1;
      if (consecutiveFails >= CONSECUTIVE_FAILS_TO_FAIL_OPEN) {
        onLog?.(`engine idle probe failed ${consecutiveFails}x (${String((error as Error).message ?? error)}); proceeding (fail-open)`);
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
 * 计数闸门：同时最多 `capacity` 个持有者；其余排队。未确认持有者按 FIFO 的
 * entries 追踪，每条标记 kind（start/wake）并带各自的宽限定时器（WAKE）。
 * confirmRunning（subagent/start）把最旧的 entry 转运行；vacate（subagent/end）
 * 释放一个运行槽。看门狗都 fail-open，绝不卡死队列。
 */
class SubagentGate {
  #capacity: number;
  #running = 0; // 已确认运行的槽（含孤儿）
  #entries: { kind: "start" | "wake"; at: number; timer?: ReturnType<typeof setTimeout> }[] = [];
  #waiters: { wait: Promise<void>; cancelled: boolean; kind: "start" | "wake"; resolve: () => void; cancel: () => void }[] = [];
  #watchdogMs: number;
  #wakeHoldMs: number;
  #runningStaleMs: number;
  #onWarn: (msg: string) => void;
  #launchTimer: ReturnType<typeof setTimeout> | undefined;
  #staleTimer: ReturnType<typeof setTimeout> | undefined;
  #disposed = false;

  constructor(
    capacity: number,
    watchdogMs: number,
    wakeHoldMs: number,
    runningStaleMs: number,
    onWarn: (msg: string) => void,
  ) {
    this.#capacity = capacity;
    this.#watchdogMs = watchdogMs;
    this.#wakeHoldMs = wakeHoldMs;
    this.#runningStaleMs = runningStaleMs;
    this.#onWarn = onWarn;
  }

  get active(): number {
    return this.#entries.length + this.#running;
  }

  /** 把容量 clamp 到引擎真实并发数（探测结果）。只会调低，不会超过配置值。 */
  setCapacity(capacity: number): void {
    if (capacity >= 1 && capacity < this.#capacity) {
      this.#capacity = capacity;
      this.#pump();
    }
  }

  /** 立即放行则返回 undefined；否则返回可取消的排队句柄。kind 为 'start'|'wake'。 */
  acquire(kind: "start" | "wake"): { promise: Promise<void>; cancel: () => void } | undefined {
    if (this.#entries.length + this.#running < this.#capacity) {
      this.#armEntry(kind);
      return undefined;
    }
    let resolve!: () => void;
    const promise = new Promise<void>((res) => {
      resolve = res;
    });
    const entry = { wait: promise, cancelled: false, kind, resolve: () => {
      if (!entry.cancelled) resolve();
    }, cancel: () => {
      entry.cancelled = true;
      const idx = this.#waiters.indexOf(entry);
      if (idx >= 0) this.#waiters.splice(idx, 1);
    } };
    this.#waiters.push(entry);
    return { promise, cancel: () => entry.cancel() };
  }

  /** 压入一个未确认持有者；WAKE 附带其独立宽限定时器。 */
  #armEntry(kind: "start" | "wake"): void {
    const entry: { kind: "start" | "wake"; at: number; timer?: ReturnType<typeof setTimeout> } = { kind, at: Date.now() };
    if (kind === "wake" && this.#wakeHoldMs > 0) {
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
    this.#entries.push(entry);
    this.#armLaunch();
  }

  confirmRunning(): void {
    if (this.#entries.length > 0) {
      const entry = this.#entries.shift();
      if (entry.timer !== undefined) clearTimeout(entry.timer);
      this.#running += 1;
    } else {
      // 孤儿 start：被 send_message 唤醒的子代理等，占用容量。引擎空闲闸门是
      // 真正的并发防线；这里保持计数诚实。
      this.#running += 1;
      if (this.#running > this.#capacity) {
        this.#onWarn?.(`orphan subagent/start pushed running to ${this.#running} (capacity ${this.#capacity}); engine-idle gate remains the concurrency guard`);
      }
    }
    this.#armStale();
  }

  vacate(): void {
    if (this.#running > 0) {
      this.#running -= 1;
    } else if (this.#entries.length > 0) {
      const entry = this.#entries.shift();
      if (entry.timer !== undefined) clearTimeout(entry.timer);
    } else {
      return;
    }
    if (this.#running === 0) this.#disarmStale();
    this.#pump();
  }

  releaseUnconfirmed(): boolean {
    if (this.#entries.length > 0) {
      const entry = this.#entries.shift();
      if (entry.timer !== undefined) clearTimeout(entry.timer);
      this.#pump();
      return true;
    }
    return false;
  }

  /** 启动看门狗：未确认 START 槽被占过久且后面有排队者时才释放；否则保留重 arm。 */
  #armLaunch(): void {
    this.#disarmLaunch();
    if (this.#watchdogMs <= 0) return;
    this.#launchTimer = setTimeout(() => {
      this.#launchTimer = undefined;
      if (this.#entries.length > 0) {
        const oldest = this.#entries[0];
        if (this.#waiters.length > 0
          && oldest.kind === "start"
          && Date.now() - oldest.at >= this.#watchdogMs) {
          this.#onWarn?.(`unconfirmed START slot held ${this.#watchdogMs}ms and a call is blocked; force-releasing it (fail-open)`);
          this.releaseUnconfirmed();
        }
        if (this.#entries.length > 0) this.#armLaunch();
      }
    }, this.#watchdogMs);
  }

  #disarmLaunch(): void {
    if (this.#launchTimer !== undefined) {
      clearTimeout(this.#launchTimer);
      this.#launchTimer = undefined;
    }
  }

  /** running 槽 stale watch：长时间无 end 且后面有排队者时强制 vacate 一个。 */
  #armStale(): void {
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

  #disarmStale(): void {
    if (this.#staleTimer !== undefined) {
      clearTimeout(this.#staleTimer);
      this.#staleTimer = undefined;
    }
  }

  /** 容量允许时放行排队持有者。 */
  #pump(): void {
    if (this.#disposed) return;
    while (this.#waiters.length > 0 && this.#entries.length + this.#running < this.#capacity) {
      const entry = this.#waiters.shift()!;
      if (entry.cancelled) continue;
      this.#armEntry(entry.kind);
      entry.resolve();
    }
  }

  /** 插件停用/HMR：清定时器、拒绝新放行。in-flight 钩子继续 next()（fail-open）。 */
  dispose(): void {
    this.#disposed = true;
    this.#disarmLaunch();
    this.#disarmStale();
    for (const entry of this.#entries) {
      if (entry.timer !== undefined) clearTimeout(entry.timer);
    }
    this.#entries = [];
  }
}

/** 调用方是否本身是子代理（子代理发出的调用）。 */
function isSubagentCaller(exec: any): boolean {
  try {
    return !!(exec && exec.agent && exec.agent.session && exec.agent.session.header
      && exec.agent.session.header.origin === "subagent");
  } catch {
    return false;
  }
}

/** 排队等待超时即放行（fail-open），防止闸门卡死委托。 */
function raceTimeout(wait: Promise<void>, timeoutMs: number): Promise<boolean> {
  if (timeoutMs <= 0) return Promise.resolve(false);
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(true), timeoutMs);
    wait.then(() => {
      clearTimeout(timer);
      resolve(false);
    });
  });
}

/**
 * 插件入口。工具分类：
 * - START（subagent/subagent_fork/team_delegate）：排队占位 → 延迟 → 等引擎空闲
 *   → 启动；槽位持有到 subagent/end。
 * - WAKE（send_message）：主代理发起时占预留槽 + 延迟 + 等引擎空闲 + 投递，投递后
 *   由被唤醒子代理的 end 或 per-entry 宽限释放；子代理身份发起直通。
 * - 子代理身份发起的任何工具调用直通（避免自我等待死锁）。
 * - INTERRUPT（interrupt_agent）：立即直通。
 * team_roles/team_find 是纯查询工具（不启动子代理），不拦截；
 * workflow 按设计排除（脚本编排器会扇出大量子代理）。
 */
export function apply(ctx: Context, config: ConfigType): void {
  const log = (level: "info" | "warn", msg: string) => ctx.logger[level](`dsh-subagent-serializer: ${msg}`);
  const engineUrl = String(config.engineUrl || "").trim();
  const probeEnabled = engineUrl.length > 0;

  const gate = new SubagentGate(
    config.maxConcurrency,
    config.watchdogMs,
    config.wakeHoldMs,
    config.runningStaleMs,
    (msg) => log("warn", msg),
  );
  ctx.effect(() => () => gate.dispose(), "dsh-subagent-serializer.gate");
  const startTools = new Set(config.toolNames);
  const wakeTools = new Set(config.wakeToolNames);
  const interruptTools = new Set(config.interruptToolNames);

  // 熔断：引擎连续不可达后，本进程内停用每次调用的 idle 等待（fail-open）。
  let idleWaitDisabled = false;

  ctx.on("subagent/start", () => {
    gate.confirmRunning();
  });

  ctx.on("subagent/end", () => {
    gate.vacate();
  });

  // 异步引擎探测（fail-open，不阻塞加载）：确认引擎真实并发数并 clamp 容量。
  if (probeEnabled) {
    (async () => {
      const slots = await probeEngineSlots(engineUrl, config.probeTimeoutMs, (msg) => log("warn", msg));
      if (slots !== undefined && slots < config.maxConcurrency) {
        gate.setCapacity(slots);
        log("info", `engine reports total_slots=${slots}; maxConcurrency clamped from ${config.maxConcurrency} to ${slots}`);
      } else if (slots !== undefined) {
        log("info", `engine probe: total_slots=${slots}; gate capacity stays at ${config.maxConcurrency}`);
      } else {
        idleWaitDisabled = true;
      }
    })();
  }

  ctx.on("tools/execute", async (exec: any, next: () => Promise<unknown>) => {
    const name = exec === undefined ? undefined : exec.name;
    if (name === undefined) return next();

    if (interruptTools.has(name)) {
      // 中断是停止操作：立即执行。等空闲会死锁（目标必然在运行中）；
      // 它引发的 subagent/end 被闸门吸收并释放容量。
      return next();
    }

    const isStart = startTools.has(name);
    const isWake = wakeTools.has(name);
    if (!isStart && !isWake) return next();

    // 子代理身份发起的调用直通（回传结果给父代理 / 派孙代理 / 协调器委托）：
    // 它已占用自己的 running 槽，任何再占槽的工具对它都是自我等待（死锁）。
    if (isSubagentCaller(exec)) return next();

    let holdingSlot = false;

    // Phase 1: 门控（acquire / 延迟 / 引擎空闲 / arm 看门狗）。插件内部错误
    // 只记日志后仍调 next()（fail-open），绝不让自身的簿记破坏委托。
    try {
      const kind = isStart ? "start" : "wake";
      const handle = gate.acquire(kind);
      if (handle !== undefined) {
        log("info", `${name} queued (${gate.active} busy); waiting for a free slot`);
        const timedOut = await raceTimeout(handle.promise, config.waitTimeoutMs);
        if (timedOut) {
          handle.cancel();
          log("warn", `${name} waited more than ${config.waitTimeoutMs}ms for a slot; proceeding anyway (fail-open)`);
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

      if (probeEnabled && config.waitForEngineIdle && !idleWaitDisabled) {
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
    } catch (error) {
      log("warn", `${name}: plugin gate error (${String((error as Error).message ?? error)}); proceeding (fail-open)`);
      if (holdingSlot) gate.releaseUnconfirmed();
      holdingSlot = false;
    }

    // Phase 2: 真正的工具调用。只有工具自身错误被 rethrow；失败时释放未确认槽。
    try {
      return await next();
    } catch (error) {
      if (holdingSlot) {
        if (!gate.releaseUnconfirmed()) {
          log("info", `${name}: slot already confirmed; it will be released by subagent/end`);
        }
      }
      throw error;
    }
  });
}

export default { name, inject, Config, apply };
