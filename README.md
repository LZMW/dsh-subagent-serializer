# dsh-subagent-serializer

**为单并发本地推理引擎设计的 DeepSeek Harness 插件**：让子代理（subagent）延迟启动，并强制同一时刻最多 N 个子代理在运行，避免主代理与子代理同时向推理引擎发请求导致上下文反复切换、速度骤降。

## 解决什么问题

你的本地部署 AI 推理引擎只支持单并发：主代理和子代理同时生成时，引擎不得不在多个上下文之间反复加载切换，响应变得极慢甚至串扰。引擎通常自带 FIFO 排队（一次只服务一个请求），但排队 = 等待 + 切换上下文，仍然很慢。

本插件从**硬约束**层面解决（不依赖模型自觉）：

1. **延迟启动**：主代理调用委托工具时，工具调用会先"悬停" `delayMs`（默认 30 秒），之后才真正执行。给主代理留出停止工作的缓冲，也避免与引擎当前正在处理的请求撞车。
2. **尽力串行**：插件内部维护一个并发闸门（默认容量 1）。正常情况下同一时刻只有一个子代理在运行，后续调用自动排队，前一个 `subagent/end` 事件到达后才放行下一个。
3. **等引擎空闲才启动**（可开关）：真正启动前轮询引擎 `/slots`，等所有槽位空闲才放行——尽力避免把子代理丢进引擎的 FIFO 排队中。

> **重要边界**：插件的串行保证是"**尽力而为**"而非绝对锁。以下情况会短暂超并发或降级为固定延迟：引擎探测/空闲检查失败（fail-open）、排队超时（waitTimeoutMs）、事件归属限制（见"已知限制"）。插件保证的是**绝不永久卡死委托**（每个等待点都有超时兜底）。

> **已知限制（事件归属）**：`subagent/start`/`subagent/end` 事件不带可关联到具体槽位的 session/run 标识，闸门按匿名 FIFO 计数。在极少数看门狗介入的窗口，迟到的 start 可能被记到继任者槽位上、`#running` 短暂超过容量；引擎空闲闸门仍是真正的并发防线，计数会在下一次 end 时自愈。不要把闸门当作看门狗介入下的硬锁。

> **不拦截**：`workflow`（脚本编排器会扇出大量子代理，属批处理语义）、`team_roles`/`team_find`（纯查询，不触发推理）。子代理身份发起的任何调用都直通（见工具分类表）。

## 工具分类与策略

| 类别 | 工具（默认） | 策略 |
|---|---|---|
| **START**（启动） | `subagent` / `subagent_fork` / `team_delegate` | 排队占位 → 延迟 → 等引擎空闲 → 启动；槽位持有到 `subagent/end` |
| **WAKE**（唤醒，主代理发起） | `send_message` | 同样占位（预留槽）→ 延迟 → 等引擎空闲 → 投递；**宽限在投递时才起表**，保持预留槽到被唤醒子代理的 `subagent/end`，或各自独立的 `wakeHoldMs` 宽限到期 |
| **子代理身份发起的调用** | 任意上述工具 | **跳过闸门占位与延迟，但保留引擎空闲等待**——子代理已占自己的 running 槽，再占槽会自我等待（回传结果/派孙代理/协调器委托等场景会死锁到超时，故必须跳过占位）；保留引擎空闲检查使孙代理/协调器委托仍不与引擎当前生成撞车 |
| **INTERRUPT**（中断） | `interrupt_agent` | 立即直通（停止操作必须即刻生效；其引发的 `subagent/end` 被闸门吸收并释放槽位） |

## 版本与 DSH 兼容性

| DSH | 本插件 | 说明 |
|---|---|---|
| **0.2.1-rc.1 及以上** | 0.2.2+ | 0.2.1 把委派改成「工具在 activation 建立时立即返回 childId，子代理在后台跑完再结算」；闸门仍以 `subagent/start` → `subagent/end` 为界，槽位跟踪的是**真实子代理运行**而不是工具调用，行为不变（已对 0.2.1-rc.2 源码复核） |
| 0.2.0 及更早 | 0.2.1 | 同上，工具调用本身会阻塞到子代理结束 |

> 顺带提醒：0.2.1 移除了 `cwd` 提示变量，`{{cwd}}` 已不可用（工作目录由 `dsh-working-directory` 作为必需运行时上下文注入）。
> 本插件的 `templates/agents.md` 与示例配置不涉及该变量，无需改动。

## 工作原理（对 DSH 源码的依赖）

| 机制 | 说明 |
|---|---|
| `tools/execute` waterfall 钩子 | 每次工具调用都会经过它；插件按工具类别拦截：START/WAKE 先排队 + 延迟 + 等引擎空闲再放行，INTERRUPT 直通 |
| `subagent/start` / `subagent/end` 生命周期事件 | `@deepseek-ai/dsh-subagent` 在子代理 activation 建立/结算时各发一次事件；`subagent/end` 释放并发槽位。0.2.1 起工具在 activation 建立时即返回，因此"工具已返回"不等于"子代理已结束"——槽位以 end 为准。`team_delegate` 内部同样通过 `ctx.subagents` 启动子代理，自动被覆盖。`send_message` 唤醒的 continuable 子代理按"孤儿 start"计数 |
| 引擎探测（可选，fail-open） | 启动时 `GET /props` 读 `total_slots`，**自动把闸门容量钳制到引擎真实并发数**（如单并发引擎钳到 1）；每次 START/WAKE 前轮询 `/slots`，**等引擎所有槽位空闲才放行**。用 Node 内置 `fetch`（`ctx.web` 拒绝 loopback 地址，不能用）。任何探测失败自动回退固定延迟 |
| 启动看门狗（watchdogMs） | START 槽在**真正发起 spawn 之后**（延迟与空闲等待完成，`markSpawning` 标记）仍未确认且有排队者被阻塞时才强制释放——延迟/空闲等待阶段的合法持槽不会被看门狗打断；无人等待时保留并继续监控，慢启动的后台子代理不会被误杀 |
| WAKE 宽限（wakeHoldMs） | 主代理 send_message 的预留槽在投递后若被唤醒子代理始终未确认，宽限到期自动释放，避免槽位被人工占住 |
| running 过期（runningStaleMs） | 运行中的槽位长时间未见 `subagent/end`（事件丢失）且有排队者时强制 vacate，防止计数永久泄漏 |
| 排队超时（waitTimeoutMs） | fail-open：排队等待超过上限仍无空槽时直接放行（宁可短暂并发，不可永久挂起） |
| fail-open 总原则 | **插件内部任何错误只记日志、不阻断工具调用**；只有工具自身的错误才被透传 |

## 安装

插件是标准 Cordis host 插件（npm 包格式），通过 DSH 的**插件管理器**安装。

方式一（推荐，本地 tarball）：

```powershell
cd dsh-subagent-serializer
npm pack
# 得到 dsh-subagent-serializer-0.2.1.tgz
```

然后在 DSH 插件管理器（设置 → 插件）中安装该 tarball。

方式二（本地路径目录）：

```powershell
# 插件管理器支持安装绝对路径目录
# 例如：H:\采集数据存储\网文大课产品\dsh-subagent-serializer
```

> 依赖：运行时只依赖 `@deepseek-ai/schemastery`（配置 schema），DSH 桌面版已内置；`@deepseek-ai/cordis` 仅类型引用。两者作为 peerDependency 声明，无需额外安装。

## 配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `delayMs` | number | `30000` | 委托调用真正执行前的延迟毫秒数 |
| `toolNames` | string[] | `["subagent","subagent_fork","team_delegate"]` | START 类工具（排队占位 + 延迟 + 持有到结束） |
| `wakeToolNames` | string[] | `["send_message"]` | WAKE 类工具（主代理发起时预留槽；子代理发起时直通） |
| `interruptToolNames` | string[] | `["interrupt_agent"]` | INTERRUPT 类工具（立即直通） |
| `maxConcurrency` | number | `1` | 同时允许运行的最大子代理数（会被引擎探测钳制，见下） |
| `watchdogMs` | number | `120000` | 启动看门狗：未确认 START 槽被占该时长且有排队者时释放 |
| `waitTimeoutMs` | number | `600000` | 排队等待空槽/空闲的超时时间，超时后 fail-open 直接放行 |
| `engineUrl` | string | `"http://127.0.0.1:8080"` | 引擎基础地址（Strata 默认）。设为 `""` 可关闭引擎探测 |
| `waitForEngineIdle` | boolean | `true` | START/WAKE 前等待引擎所有槽位空闲（`/slots` 的 `is_processing !== true`，缺失/0 也视为空闲） |
| `enginePollMs` | number | `2000` | 等待空闲时的轮询间隔 |
| `probeTimeoutMs` | number | `3000` | 单次 `/props`、`/slots` 请求超时 |
| `idleTimeoutMs` | number | `600000` | 等待引擎空闲的总超时，超时后 fail-open 放行 |
| `wakeHoldMs` | number | `15000` | WAKE 预留槽投递后未确认时的宽限，到期自动释放 |
| `runningStaleMs` | number | `600000` | running 槽长时间无 end 且有排队者时强制 vacate |

**引擎探测的自动行为**：
- 启动时读 `/props.total_slots`；若小于 `maxConcurrency`，**自动把闸门容量钳制为 `total_slots`**（单并发引擎 → 1）。日志会打印如 `engine reports total_slots=1; maxConcurrency clamped from 2 to 1`。
- 探测**只在插件加载时做一次**；引擎之后重启改变 `total_slots` 不会被重新探测。启动探测失败（例如 `/props` 缺失）**不会**关闭空闲检查——`/slots` 可能仍然可用。
- 空闲检查有**熔断**：空闲轮询连续 3 次失败后，本进程内停用 idle 等待，降级为固定延迟（fail-open）。
- 探测或空闲检测**任何失败**都会打日志并回退到固定延迟（fail-open），绝不影响子代理委托。
- 插件用 Node 内置 `fetch` 探测引擎——不能用 `ctx.web`，因为 DSH 的 web provider 出于安全拒绝 loopback/私有地址。
- **没有引擎或不需要探测时，请把 `engineUrl` 设为 `""`**，否则每次委托调用都会先做空闲检查（引擎不可达时最多耗 3 次探测超时）。

示例配置（Strata 单并发引擎，延迟 60 秒）：

```json
{
  "delayMs": 60000,
  "toolNames": ["subagent", "subagent_fork", "team_delegate"],
  "wakeToolNames": ["send_message"],
  "interruptToolNames": ["interrupt_agent"],
  "maxConcurrency": 1,
  "watchdogMs": 120000,
  "waitTimeoutMs": 900000,
  "engineUrl": "http://127.0.0.1:8080",
  "waitForEngineIdle": true,
  "enginePollMs": 2000,
  "probeTimeoutMs": 3000,
  "idleTimeoutMs": 600000,
  "wakeHoldMs": 15000,
  "runningStaleMs": 600000
}
```

> 提示：`watchdogMs` 只约束"真正发起 spawn 之后"的未确认窗口（延迟/空闲等待不计入），无需覆盖 `delayMs`；按引擎最差单次处理时长（Strata + 262K 上下文可能 40–300 秒）留足余量即可。`waitTimeoutMs`/`idleTimeoutMs`/`runningStaleMs` 同样按最差处理时长留余量。

## 与 agents.md 的配合

插件做的是**硬约束**；`agents.md` 做的是**软约束**（引导主代理行为）。两者互补，建议同时使用。配套模板见 [`templates/agents.md`](templates/agents.md)，直接复制到你的会话/工作区即可。

## 目录结构

```
dsh-subagent-serializer/
├── package.json            # 插件清单（npm 包格式）
├── lib/index.js            # 运行时入口（ESM，无构建依赖）
├── lib/index.d.ts          # 类型声明
├── src/index.ts            # 类型化源码（维护参考，行为与 lib 一致）
├── templates/agents.md     # agents.md 约束模板
├── examples/
│   └── basic-mode.patch.yml # 示例：把插件挂进自定义 agent preset（脱敏）
└── README.md
```

## 示例：把插件挂进自定义 agent preset（只对某模式启用）

默认情况下插件作为独立插件启用后对所有会话生效。如果你希望**只在某个 agent 模式（preset）下启用**——例如创建一个"基础模式"（无 `workflow` 工具、启用单并发串行），而系统"标准模式"不受影响——参考 [`examples/basic-mode.patch.yml`](examples/basic-mode.patch.yml)：

- 它声明了一个完整 preset（`id: basic-mode`），内容与 DSH 的 standard preset 一致，但**移除**了 `dsh-workflow-ptc` / `dsh-tool-workflow`，并**在 preset 内部**挂载了 `dsh-subagent-serializer`。
- 把该文件的 `- insert:` 列表追加到你的 profile patch（如 `<DSH_HOME>/profiles/desktop/cordis.patch.yml`），重启 DSH 后新建会话即可在模式选择器里看到"基础模式"。
- preset 是共享常驻挂载：所有选择该模式的会话共享同一个串行闸门，符合单并发语义。
- 该文件为**脱敏示例**：不含任何个人 API 密钥、模型路由或机器路径，请按你自己的环境调整模型/provider 与 `engineUrl`。

## 行为边界与注意

1. **延迟语义**：延迟加在 `subagent` 等工具调用上——主代理发出调用后，工具调用本身会阻塞 `delayMs` 才真正执行。前台/后台模式都生效。
2. **前台模式**（`run_in_background: false`）：主代理本来就会等子代理结果，额外多等 `delayMs` 是预期成本。
3. **批量委托的延迟叠加**：排队批量委托时，每个入队者在放行后才睡满 `delayMs`（批量 N 个 ≈ N×`delayMs` 纯睡眠）。这是设计使然——每个子代理启动前都要给主代理收尾缓冲；若批量任务多，可把 `delayMs` 调小。
4. **send_message 发给活跃子代理**：主代理对正在运行的子代理发消息（steer）会排在它自己的槽位之后、等引擎空闲再投递——对引擎友好，但实际是"等闸门槽位"而非仅"等引擎空闲"，典型结果是子代理结束后才投递（steer 变成事后追问）。若你的工作流高度依赖实时 steer，可把 `send_message` 从 `wakeToolNames` 移除（逃生阀）。
5. **workflow / team_roles / team_find**：有意不拦截（见上"重要边界"）。
6. **fail-open 原则**：任何内部错误只记日志、不阻断工具调用；插件自身故障不会破坏子代理委托。
7. **多会话并发**：闸门按插件实例（通常全局一个）计数。若你的本地引擎连"多个主代理会话同时跑"都扛不住，需要你在会话层再串行。

## 开发与测试

```powershell
node --check lib/index.js
```

## License

MIT
