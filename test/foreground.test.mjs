// Regression test: foreground blocking (dsh-subagent-serializer >= 0.2.4).
//
// DSH >= 0.2.1 returns a child id from the delegation tools as soon as the
// activation is accepted and lets the calling agent keep generating. This plugin
// must hold the tool call open until the child stops working, or a
// single-concurrency engine sees the parent and the child at the same time.
//
// No harness required: the test drives the plugin's `tools/execute` listener
// with a minimal cordis-shaped ctx. Run with `node test/foreground.test.mjs`.
//
// Case map (each case targets one release signal or one failure mode):
//   1  blocks until subagent/end                      (event signal)
//   2  team_delegate background result blocks          (no rewrite is possible:
//                                                      arguments are deep-frozen)
//   3  idle-but-pending inbox does NOT count as done   (the 0.2.1 parked child)
//   4  foreground:false returns immediately            (opt-out regression)
//   5  foregroundTimeoutMs releases the call           (safety valve)
//   6  two quiet samples release without an event      (missed-event fallback)
//   7  wake is keyed to its OWN target id              (no cross-child release)
//   8  abort during the wait releases promptly         (cancellation)
import assert from "node:assert/strict";
import mod from "../lib/index.js";

const logs = [];
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function baseConfig(overrides = {}) {
  return {
    delayMs: 0,
    toolNames: ["subagent", "subagent_fork", "team_delegate"],
    wakeToolNames: ["send_message"],
    interruptToolNames: ["interrupt_agent"],
    maxConcurrency: 1,
    watchdogMs: 120000,
    waitTimeoutMs: 600000,
    engineUrl: "", // no engine probe: the delay/gate wiring is not under test here
    waitForEngineIdle: true,
    enginePollMs: 400, // -> poll interval 200ms, so a "quiet" sample costs 200ms
    probeTimeoutMs: 3000,
    idleTimeoutMs: 600000,
    wakeHoldMs: 15000,
    runningStaleMs: 600000,
    foreground: true,
    foregroundTimeoutMs: 1800000,
    foregroundWakes: false,
    ...overrides,
  };
}

/** Apply the plugin to a fake ctx and dispatch one tool call through its hook. */
function makeCall(config, services, { name, args = {}, result, signal }) {
  const handlers = new Map();
  const ctx = {
    logger: { info: (m) => logs.push(`INFO ${m}`), warn: (m) => logs.push(`WARN ${m}`) },
    effect: (fn) => { fn(); },
    on: (n, f) => {
      const list = handlers.get(n) ?? [];
      list.push(f);
      handlers.set(n, list);
    },
    get: (n) => services[n],
    emit: (n, info) => { for (const f of handlers.get(n) ?? []) f(info); },
  };
  mod.apply(ctx, config);
  const executes = handlers.get("tools/execute") ?? [];
  assert.equal(executes.length, 1, "plugin must register exactly one tools/execute listener");
  const exec = {
    name,
    arguments: Object.freeze(args),
    agent: { id: "parent", session: { header: { origin: "user" } } },
    ...(signal === undefined ? {} : { signal }),
  };
  let chain = () => Promise.resolve(result);
  for (const handler of [...executes].reverse()) {
    const downstream = chain;
    chain = () => handler(exec, downstream);
  }
  return { ctx, exec, call: chain() };
}

/** agents service stub: id -> { status, inbox } */
const agentsOf = (table) => ({ get: (id) => table[id] });
const idleAgent = { status: "idle", inbox: { nextTurn: [], nextStep: [] } };
const runningAgent = { status: "running", inbox: { nextTurn: [], nextStep: [] } };
const parkedAgent = { status: "idle", inbox: { nextTurn: ["queued turn"], nextStep: [] } };

// 1. `subagent` must not resolve before the child's subagent/end.
{
  logs.length = 0;
  const { ctx, call } = makeCall(baseConfig(), {}, {
    name: "subagent",
    result: { isError: false, value: { kind: "activation", subagentId: "child-1" } },
  });
  let settled = false;
  call.then(() => { settled = true; });
  await sleep(60);
  assert.equal(settled, false, "subagent call must stay open until subagent/end");
  ctx.emit("subagent/end", { id: "child-1", runId: "run-1", provider: "spawn", local: true, stopReason: "completed" });
  const value = await call;
  assert.deepEqual(value.value, { kind: "activation", subagentId: "child-1" });
  assert.ok(logs.some((l) => l.includes("holding the call until subagent child-1")), "logs the hold");
  assert.ok(logs.some((l) => l.includes("released by subagent/end")), "logs the release signal");
  console.log("case 1 OK: blocks until subagent/end");
}

// 2. team_delegate background result blocks; arguments are never rewritten.
{
  logs.length = 0;
  const args = { subagent_type: "x", description: "d", prompt: "p" };
  const { ctx, exec, call } = makeCall(baseConfig(), {}, {
    name: "team_delegate",
    args,
    result: { isError: false, value: { kind: "continuable", subagentId: "child-2" } },
  });
  let settled = false;
  call.then(() => { settled = true; });
  await sleep(60);
  assert.equal(settled, false, "background delegation must block");
  assert.equal(exec.arguments.run_in_background, undefined, "arguments must not be tampered with");
  assert.ok(!logs.some((l) => l.includes("forced to false")), "no argument rewrite is attempted");
  ctx.emit("subagent/end", { id: "child-2" });
  await call;
  assert.equal(settled, true);
  console.log("case 2 OK: background delegation blocks, no argument rewrite");
}

// 2b. a native foreground result (model itself passed run_in_background:false)
//     carries the child's outcome already -> no extra wait.
{
  logs.length = 0;
  const { call } = makeCall(baseConfig(), {}, {
    name: "team_delegate",
    args: { subagent_type: "x", description: "d", prompt: "p", run_in_background: false },
    result: { isError: false, value: { kind: "foreground", message: "done" } },
  });
  const value = await call;
  assert.equal(value.value.kind, "foreground");
  assert.ok(logs.some((l) => l.includes("no child id in the tool result")), "skips the wait");
  console.log("case 2b OK: native foreground result skips the wait");
}

// 3. A parked child (idle, pending inbox) is NOT done — the 0.2.1 trap that
//    waitForChildren skips and a status-only guard would also mis-read.
{
  logs.length = 0;
  const services = { agents: agentsOf({ "child-3": parkedAgent }) };
  const { ctx, call } = makeCall(baseConfig(), services, {
    name: "subagent",
    result: { isError: false, value: { kind: "activation", subagentId: "child-3" } },
  });
  let settled = false;
  call.then(() => { settled = true; });
  await sleep(700); // several quiet polls would have released a status-only guard
  assert.equal(settled, false, "idle + pending inbox must keep the call blocked");
  ctx.emit("subagent/end", { id: "child-3" });
  await call;
  console.log("case 3 OK: parked child (idle + pending inbox) still blocks");
}

// 4. `foreground: false` restores the plain DSH background behaviour.
{
  const { call } = makeCall(baseConfig({ foreground: false }), {}, {
    name: "subagent",
    result: { isError: false, value: { kind: "activation", subagentId: "child-4" } },
  });
  const value = await call;
  assert.equal(value.value.subagentId, "child-4");
  console.log("case 4 OK: foreground:false returns immediately");
}

// 5. `foregroundTimeoutMs` releases a child that never settles (fail-open).
{
  logs.length = 0;
  const { call } = makeCall(baseConfig({ foregroundTimeoutMs: 120 }), {}, {
    name: "subagent",
    result: { isError: false, value: { kind: "activation", subagentId: "child-5" } },
  });
  const value = await call;
  assert.equal(value.value.subagentId, "child-5");
  assert.ok(logs.some((l) => l.includes("did not settle within 120ms")), "logs the timeout release");
  console.log("case 5 OK: timeout releases the call");
}

// 6. Missed event: two consecutive quiet samples release the call (no event).
{
  logs.length = 0;
  const services = { agents: agentsOf({ "child-6": idleAgent }) };
  const { call } = makeCall(baseConfig(), services, {
    name: "subagent",
    result: { isError: false, value: { kind: "activation", subagentId: "child-6" } },
  });
  const value = await call;
  assert.equal(value.value.subagentId, "child-6");
  assert.ok(logs.some((l) => l.includes("without a new subagent/end")), "logs the idle fallback");
  console.log("case 6 OK: idle fallback releases without an event");
}

// 7. Wake is keyed to its own target: another child's end must not release it.
{
  logs.length = 0;
  const services = { agents: agentsOf({ "agent-7": runningAgent }) };
  const { ctx, call } = makeCall(baseConfig({ foregroundWakes: true }), services, {
    name: "send_message",
    args: { agent_id: "agent-7", message: "keep going" },
    result: { isError: false, value: { messageId: "m1" } },
  });
  let settled = false;
  call.then(() => { settled = true; });
  await sleep(60);
  assert.equal(settled, false, "wake blocks while its target runs");
  ctx.emit("subagent/end", { id: "someone-else" });
  await sleep(200);
  assert.equal(settled, false, "an unrelated child's end must not release the wake");
  ctx.emit("subagent/end", { id: "agent-7" });
  await call;
  assert.equal(settled, true);
  assert.ok(logs.some((l) => l.includes("holding the wake until agent agent-7")), "logs the keyed wake");
  console.log("case 7 OK: wake is keyed to its own target id");
}

// 8. Abort during the wait releases promptly instead of waiting out the timeout.
{
  logs.length = 0;
  const controller = new AbortController();
  const { call } = makeCall(baseConfig({ foregroundTimeoutMs: 60000 }), {}, {
    name: "subagent",
    result: { isError: false, value: { kind: "activation", subagentId: "child-8" } },
    signal: controller.signal,
  });
  let settled = false;
  call.then(() => { settled = true; });
  await sleep(60);
  assert.equal(settled, false);
  controller.abort();
  const value = await call;
  assert.equal(value.value.subagentId, "child-8");
  assert.ok(logs.some((l) => l.includes("aborted after")), "logs the abort release");
  console.log("case 8 OK: abort releases the call");
}

console.log("\nALL CASES PASSED");
process.exit(0);
