# Crypto Arb Scanner

一个面向 **CEX / Perp DEX 跨交易所套利研究** 的 FastAPI 项目。

当前项目重点不是自动交易，而是构建一个稳定的 **opportunity discovery / replay / execution sizing** 研究框架，用于持续发现、解释、评估跨交易所永续合约套利机会。

---

## Project Status

当前项目处于：

**Research / Opportunity Discovery Stage**

已具备：

- 多交易所永续行情抓取与归一化
- funding / mark / index / open interest 等核心字段标准化
- data quality gating
- 跨交易所套利机会扫描
- replay preview / profile compare
- execution sizing / account-state-aware 评估

当前**不包含自动下单执行**，也不以 production trading bot 为目标。

---

## Guarded Live Pilot (v1)

首个真实执行适配器试点仅支持 **Binance**，并且仍在严格保护边界后运行：

- 默认依然是非 live（`guarded_live_submit` 默认关闭）。
- 只有在全部 gate 通过 + 明确 arm 后，才会尝试走 Binance pilot live adapter。
- 当前仅支持非常窄的 route：`binance -> binance` 双腿路径。
- 混合 venue 或第二腿非 Binance 会被显式阻断并返回机器可读原因（如 `mixed_live_venue_path_not_supported_yet`）。

### Required config / credentials

- `ARB_BINANCE_API_KEY`
- `ARB_BINANCE_API_SECRET`
- 可选：`ARB_BINANCE_TRADE_BASE_URL`（默认 `https://fapi.binance.com`）
- 可选：`ARB_BINANCE_RECV_WINDOW_MS`（默认 `5000`）

凭证只从配置/环境读取，不会打印或持久化 secret。

### Arming guarded live submit

仍需满足已有 policy / account-state / credential-readiness / live-entry gate。

同时还需：

- `ARB_GUARDED_LIVE_SUBMIT_ENABLED=true`
- 若启用 arm token 校验：提供匹配的 `arm_token`

### What is intentionally not supported yet

- 多 venue live submit 编排
- 非 Binance live adapter
- position / balance live methods
- 全路径 cancel / order-status 路由编排

### Safe testing

- 默认测试流全部使用 mocked transport，不依赖真实网络与真实凭证。
- 可继续使用 preview / dry-run 路径验证流程；live pilot 测试应显式 mock outbound transport。

---

## Supported Venues

当前已接入的交易所：

- Binance
- OKX
- Hyperliquid
- Lighter

---

## Core APIs

### `GET /api/v1/snapshots`
返回多交易所归一化后的市场快照。

典型字段包括：

- exchange
- venue_type
- base_symbol
- normalized_symbol
- instrument_id
- mark_price
- index_price
- last_price
- funding_rate
- funding_rate_source
- funding_time_ms
- next_funding_time_ms
- funding_period_hours
- hourly_funding_rate
- hourly_funding_rate_bps
- open_interest_usd
- quote_volume_24h_usd
- data_quality_status
- data_quality_flags
- raw

---

### `GET /api/v1/opportunities`
基于当前 snapshots 扫描跨交易所套利机会。

输出会综合考虑：

- 价差
- funding spread
- fee assumptions
- data quality
- risk flags
- conviction / score
- execution sizing inputs

---

### `GET /api/v1/replay-preview`
对机会进行 replay 预览，用于研究假设下的机会质量与成本后表现。

---

### `GET /api/v1/replay-profile-compare`
比较不同 replay / execution profile 下，同一机会的结果差异。

---

### `GET /api/v1/meta`
返回系统元信息、支持的交易所、支持的 symbol、配置概览等。

---


## Adding a New Venue / 新增交易所标准接入流程

为了让后续交易所接入低风险、可复用，建议固定按以下步骤推进：

1. **先写 venue reality notes（现实约束说明）**  
   明确该 venue 的市场类型（CEX/DEX）、认证方式、symbol 规则、rate limit、可用接口与已知限制。
2. **在 registry 新增 `VenueDefinition`**  
   位置：`app/venues/registry.py`。先完整填写元信息和 capability 布尔位，再决定是否启用。
3. **实现并接线 market adapter**  
   位置：`app/exchanges/`。继承 `BaseMarketAdapter`（`app/exchanges/base.py`），实现 `fetch_snapshots(...)` 与 symbol 归一化。
4. **补 execution adapter scaffold 条目**  
   位置：`app/execution_adapters/`。继承 `BaseExecutionAdapter`；在未准备 live 之前保持 paper/mock 路径。
5. **补 symbol mapping 与 focused tests**  
   优先验证：symbol 正规化、capability endpoint 输出、adapter deterministic 行为、无网络依赖。
6. **最后才考虑 live execution support**  
   仅在风控、审计、回放验证、权限隔离都通过后，再把 `live_supported_now` 从 `false` 升级。

当前仓库仍保持 research/paper-first：

- `live_supported_now=false`（所有 venue）
- 不要求 exchange credentials
- 不引入 live 下单逻辑

### Why reality check before live adapters

在开始任何 live execution adapter 之前，仓库会先维护一层「venue reality check」：

- 先确认 venue 是 **CEX 私有 API 风格**（例如 Binance / OKX）还是 **signed-action / wallet-signature 风格**（例如 Hyperliquid / Lighter）。
- 用统一 metadata 标出：是否 REST 私有交易、是否依赖 signed actions、是否更适合 SDK 驱动。
- 通过只读接口暴露能力分组，先把架构边界对齐，再进入真实下单实现。

这能避免后续把不同执行模型硬塞进同一套 live adapter 抽象，并保持当前仓库继续 non-live / paper-first。

## Architecture Overview

项目当前大致分为以下几层：

### 1. Exchange Adapters
位于 `app/exchanges/`

职责：

- 调用各交易所 REST / WebSocket 数据源
- 处理 symbol 差异
- 提取 funding / mark / index / volume / OI 等字段
- 保留原始 payload 便于调试

---

### 2. Normalized Market Snapshot Layer
位于 `app/models/market.py`

核心对象是 `MarketSnapshot`，它是整个项目的统一市场数据模型。

设计目标：

- 屏蔽不同交易所字段差异
- 明确 funding 字段语义
- 支持 data quality 标记
- 便于 scanner / replay / sizing 共用

---

### 3. Opportunity Scanner
主要位于 `app/services/arbitrage_scanner.py`

职责：

- 按 symbol 聚合多交易所快照
- 两两比较 exchange pair
- 计算 price spread / funding spread / net edge
- 引入 fee、quality gate、risk flags
- 产出候选套利机会

---

### 4. Replay Layer
主要位于 `app/services/opportunity_replay.py`

职责：

- 对候选机会做 replay preview
- 评估在给定假设下的成本后表现
- 支持 profile compare
- 帮助判断“机会看起来存在”与“机会实际可研究/可执行”之间的差异

---

### 5. Execution Policy / Account-State Layer
主要位于：

- `app/services/execution_sizing_policy.py`
- `app/services/execution_account_state.py`

职责：

- 根据账户状态、风险参数、profile 评估机会的可执行性
- 决定 execution mode
- 给出 final position sizing / leverage-aware 约束结果

---

### 6. Data Quality Layer
主要位于：

- `app/services/data_quality_gate.py`
- `app/services/data_quality_rules.py`

职责：

- 对 market snapshots 和 opportunities 做质量校验
- 标记 degraded / missing / stale / inconsistent 数据
- 防止低质量数据直接进入高置信度机会结果

---

## Repository Structure

```text
app/
  core/
  exchanges/
  models/
  services/
  main.py

tests/

AGENTS.md
README.md
pyproject.toml
test_after_codex.sh
Local Development
Python Version

推荐使用：

Python 3.11

不建议使用过新的 Python 版本做主开发环境，以减少依赖兼容问题。

Create Virtual Environment
python3.11 -m venv .venv
source .venv/bin/activate
Install Dependencies
pip install -e .
Proxy / Network Notes

项目依赖外部交易所 API，本地开发通常需要代理环境。

当前推荐方式：

使用本地代理客户端
允许脚本自动读取系统代理或 fallback 端口
通过 certifi 提供稳定的 CA bundle

如果你使用本项目自带脚本，通常无需手动反复配置代理环境变量。

Recommended Workflow After Codex Changes

每次 Codex 或 GitHub 上有新改动后，推荐直接运行：

./test_after_codex.sh

该脚本负责：

拉取 main 最新代码
激活虚拟环境
自动检测代理
按需安装依赖
运行测试
启动本地 FastAPI 服务

如果成功，你会看到本地服务运行在：

http://127.0.0.1:8000

Swagger 文档地址：

http://127.0.0.1:8000/docs
Running the API Manually

如果你不想走脚本，也可以手动运行：

source .venv/bin/activate
uvicorn app.main:app --reload
Testing

运行测试：

pytest -q

当前项目应保持：

全量测试通过
API 可启动
核心 endpoints 可访问
What This Project Is Not

当前项目不是：

自动下单系统
production-ready execution bot
高频撮合系统
完整回测平台

当前目标是：

持续发现套利机会
解释机会质量
评估 replay 表现
判断执行可行性
为后续 observation / dashboard / alerting 打基础
Current Roadmap

下一阶段重点不是继续横向堆功能，而是补上研究闭环。

Priority 1: Observation / Persistence Layer

把当前一次性计算结果沉淀下来，形成历史样本。

计划内容：

opportunity observation persistence
历史记录查询
route / symbol 级别研究接口
可持续积累的研究样本
Priority 2: Research Summary APIs

将 scanner / replay / sizing 的结果做聚合分析。

计划内容：

最近 24h 机会数
replay 通过率
quality gate 拦截分布
profile compare summary
route-level statistics
Priority 3: Dashboard / Alerting

在有 observation 历史数据之后，增加：

dashboard
Telegram / Discord alerting
高频 route 监控
去重告警
Priority 4: Execution

只有在 observation / replay / sizing / quality gate 都稳定之后，才考虑更进一步的执行链路。

Design Principles
保持不同交易所差异显式化，而不是过度隐藏
funding 语义必须谨慎归一化
保留 raw payload 便于调试
允许 partial success，不因单个交易所失败而让整次请求失败
研究优先于执行
小步迭代，先做可验证的能力
Notes for Coding Agents

请优先阅读：

AGENTS.md

在做修改时遵循：

尽量小改动
不重写无关模块
不虚构不存在的功能
不提前引入 auto-trading
保持现有 API 行为稳定
