# crypto-arb-scanner

一个面向 **跨 CEX / Perp DEX 价差与资金费率套利** 的 FastAPI 项目骨架。

当前 V1 目标不是自动下单，而是先把 **四个交易所的永续合约市场数据采集** 跑通，为后续的机会扫描器、告警和模拟交易打基础。

目前接入：

- Binance USDⓈ-M Futures
- OKX Perpetual Swaps
- Hyperliquid Perps
- Lighter Perps

## 当前能力

- 提供统一的 `MarketSnapshot` 数据模型
- 通过 `/api/v1/snapshots` 拉取 `BTC / ETH / SOL` 的多交易所数据
- 返回：
  - 标记价格 / mark price
  - 指数价格 / index price（若交易所提供）
  - 资金费率 / funding rate
  - 资金费率语义标记（current / latest_reported / estimated_current 等）
  - 原始交易所 payload（便于你后面继续扩展）
- 对交易所错误做隔离：某一家失败不会让全部接口直接挂掉

## 为什么 funding_rate_source 很重要

四家交易所在“资金费率”字段上的语义并不完全相同：

- Binance `premiumIndex` 返回 `lastFundingRate`
- OKX 最理想是走 `funding-rate` WebSocket，若失败则 fallback 到 `funding-rate-history`
- Hyperliquid `metaAndAssetCtxs` 里的 `funding` 是当前 8h funding
- Lighter `market_stats` 里的 `current_funding_rate` 是“下一次 funding 的估计值”，`funding_rate` 是最近一次已发生 funding

所以代码里保留了 `funding_rate_source`，避免你后面做套利计算时把不同语义的数据硬混在一起。

## 项目结构

```text
crypto-arb-scanner/
  app/
    main.py
    core/
      config.py
      symbols.py
    models/
      market.py
    exchanges/
      base.py
      binance.py
      okx.py
      hyperliquid.py
      lighter.py
    services/
      market_data.py
  tests/
    test_symbols.py
  .env.example
  pyproject.toml
  README.md
```

## Local Development Environment

Recommended local setup on macOS:

- Python 3.11
- virtualenv under `.venv`
- start Uvicorn with the asyncio loop

Why:

- local development has been more stable with Python 3.11 than Python 3.13
- using the asyncio loop avoids local websocket/runtime compatibility issues seen with newer runtime combinations

### Install Python 3.11 on macOS

```bash
brew install python@3.11
python3.11 --version
```

### Local setup and startup (macOS recommended)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
python -m uvicorn app.main:app --reload --loop asyncio
```

## Proxy / SOCKS Support

If your environment sets `ALL_PROXY` to a `socks5://` or `socks5h://` proxy, HTTPX requires SOCKS support.

This project now includes SOCKS support via dependencies (`httpx[socks]`).

If you still see:

`Using SOCKS proxy, but the 'socksio' package is not installed`

install explicitly with:

```bash
pip install "httpx[socks]"
```

Example proxy environment variables:

```bash
export HTTP_PROXY=http://127.0.0.1:9098
export HTTPS_PROXY=http://127.0.0.1:9098
export ALL_PROXY=socks5h://127.0.0.1:9099
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
```

## API

### 健康检查

```bash
curl http://127.0.0.1:8000/healthz
```

### 获取快照

```bash
curl "http://127.0.0.1:8000/api/v1/snapshots?symbols=BTC,ETH,SOL"
```

### 查看支持的 symbol

```bash
curl http://127.0.0.1:8000/api/v1/meta
```

## 设计说明

### 1. 为什么 Binance 用 REST

V1 先用 `/fapi/v1/premiumIndex`，因为它一条接口就能拿到：

- mark price
- index price
- latest reported funding
- next funding time

对原型阶段最省心。

### 2. 为什么 OKX 同时保留 REST + WS 逻辑

OKX 的 mark price 很适合走 REST；资金费率更适合走 `funding-rate` 公共 WebSocket。

当前代码逻辑：

- 先用 REST 拿 mark price
- 再尝试用 WS 拿 current funding / next funding time
- 如果 WS 不可用，则 fallback 到 `funding-rate-history`

这样你在代理环境、区域环境下也更容易先把原型跑起来。

### 3. 为什么 Hyperliquid 用 `metaAndAssetCtxs`

因为这一条请求能同时拿到 universe 元数据和 asset contexts，后者包含：

- mark price
- oracle price
- funding
- open interest

适合做统一快照层。

### 4. 为什么 Lighter 用 read-only WebSocket

Lighter 的 `market_stats` WebSocket 直接给：

- mark_price
- index_price
- current_funding_rate
- funding_rate
- funding_timestamp

而且官方明确支持 `?readonly=true` 的只读连接，对受限地区更友好。

## 下一步建议

建议你下一轮直接做：

1. `Opportunity` 数据模型
2. 扫描逻辑：
   - 同标的跨 venue 价差
   - funding spread
   - fee/slippage 估算
3. 一个 `/api/v1/opportunities` 接口
4. 再加 Telegram 告警

## 官方文档（便于继续扩展）

- Binance Futures REST docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price
- OKX API docs: https://app.okx.com/docs-v5/en/
- Hyperliquid info endpoint docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- Lighter API docs: https://apidocs.lighter.xyz
