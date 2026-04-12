# Execution Venue Reality Check (Non-live)

Date: 2026-04-12

Purpose: capture execution-interface realities before any live adapter implementation. This document is intentionally implementation-oriented and does **not** enable live trading in this repository.

## Binance (USDⓈ-M futures)
- **Order placement interface shape**: private REST `POST /fapi/v1/order` (signed TRADE).
- **Cancellation interface shape**: private REST `DELETE /fapi/v1/order` (by `symbol` + `orderId` or `origClientOrderId`).
- **Order status interface shape**: private REST `GET /fapi/v1/order`.
- **Positions interface shape**: private REST `GET /fapi/v2/positionRisk`.
- **Balances interface shape**: private REST `GET /fapi/v3/balance`.
- **Auth/signing style**: API key header + HMAC signature on signed endpoints (`timestamp`/`recvWindow` conventions).
- **Implementation caveats**:
  - REST request/weight limits and order-rate limits must be enforced.
  - Real-time state should combine REST snapshots with user-data websocket events.
- **SDK recommendation**: optional; signing can be implemented directly but official connectors are available.

## OKX (V5)
- **Order placement interface shape**: private REST `POST /api/v5/trade/order`.
- **Cancellation interface shape**: private REST `POST /api/v5/trade/cancel-order`.
- **Order status interface shape**: private REST `GET /api/v5/trade/order` (order details).
- **Positions interface shape**: private REST `GET /api/v5/account/positions`.
- **Balances interface shape**: private REST `GET /api/v5/account/balance`.
- **Auth/signing style**: API key + secret + passphrase, with timestamped signature headers (`OK-ACCESS-*`).
- **Implementation caveats**:
  - Account mode / position mode materially changes required order params.
  - Private websocket is important for low-latency order/account state sync.
- **SDK recommendation**: optional; direct REST signing is feasible.

## Hyperliquid
- **Order placement interface shape**: signed action submitted to exchange endpoint (`action` payloads, e.g., order actions).
- **Cancellation interface shape**: signed action (cancel/cancel-by-oid style actions).
- **Order status interface shape**: info endpoint queries (e.g., open orders / user fills) rather than classic private order REST per venue conventions.
- **Positions interface shape**: info endpoint user-state query.
- **Balances interface shape**: info endpoint user-state/margin summary query.
- **Auth/signing style**: wallet-style signatures with venue-specific signing schemes (L1/user-signed action paths).
- **Implementation caveats**:
  - Correct signature construction is fragile (payload formatting/order/nonce handling).
  - Live adapters should treat this as a signed-action pipeline, not a generic CEX private REST adapter.
- **SDK recommendation**: **yes** (official docs explicitly recommend SDK usage for signing correctness).

## Lighter
- **Order placement interface shape**: signed transaction-style/API-key-authenticated order flow (SDK-oriented).
- **Cancellation interface shape**: signed cancel/cancel-all transaction flow.
- **Order status interface shape**: auth-gated API/websocket channels for order state.
- **Positions interface shape**: auth-gated account queries/channels.
- **Balances interface shape**: auth-gated account queries/channels.
- **Auth/signing style**: API key public/private pair per account index, auth token generation, nonce-managed signed payloads; some flows still require L1 wallet signature.
- **Implementation caveats**:
  - Nonce sequencing and key-index management are core integration risks.
  - Read-only auth tokens differ from trade-capable signer flows.
- **SDK recommendation**: **yes** (Python/Go SDKs are the practical path for signing and nonce handling).

## Implementation takeaway for this repo
- Binance/OKX map to **classic CEX private API adapters**.
- Hyperliquid/Lighter map to **signed-action or transaction-style adapters**.
- Therefore, execution capability metadata must distinguish classic API vs signed-action venues before any live adapter work.
- Live support in this repository remains disabled (`live_supported_now=false` across all venues).
