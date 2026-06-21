# crypto-ai-analysis

[![Tests](https://github.com/ares-b/crypto_ai_analysis/actions/workflows/tests.yml/badge.svg)](https://github.com/ares-b/crypto_ai_analysis/actions/workflows/tests.yml)
[![Docker](https://github.com/ares-b/crypto_ai_analysis/actions/workflows/docker.yml/badge.svg)](https://github.com/ares-b/crypto_ai_analysis/actions/workflows/docker.yml)
[![Image](https://img.shields.io/badge/ghcr.io-crypto__ai__analysis-blue?logo=docker)](https://github.com/ares-b/crypto_ai_analysis/pkgs/container/crypto_ai_analysis)

Daily BTC data pipeline. Ingests raw market, on-chain, macro, and sentiment signals into Apache Iceberg on S3. Runs as a Dagster user code deployment on k3s.

## Data sources

| Pipeline | Source | Signals |
|---|---|---|
| `candles` | Binance | BTC/USDT OHLCV |
| `futures` | Deribit | Funding rates, open interest, basis, long/short ratio |
| `market_metrics` | CoinGecko, CoinMetrics | Market cap, volume, stablecoin supply |
| `onchain_metrics` | CoinMetrics, blockchain.info | Active addresses, transaction volume, miner revenue |
| `exchange_flows` | CryptoQuant | Exchange inflow/outflow |
| `etf_flows` | CryptoQuant | Spot BTC ETF flows |
| `cot_positioning` | CFTC | Commitment of Traders (BTC futures) |
| `macro_series` | FRED | DXY, M2, Fed Funds Rate |
| `macro_calendar` | FRED | Economic calendar events |
| `sentiment_index` | Alternative.me | Fear & Greed index |
| `market_news` | — | Raw news items |

## Layout

```
src/
  core/           # library — no app imports
  pipelines/      # fetch + run logic, no Dagster
  orchestration/  # Dagster assets only
  schemas.py      # ALL_SPECS — auto-discovers IcebergRow subclasses at boot
```

`pipelines/` never imports from `orchestration/`. `core/` never imports from either.

## Dev setup

```bash
uv sync
uv run pytest tests/ --cov=src --cov-report=term-missing
```

## Required env vars

```
CRYPTOQUANT_API_KEY
FRED_API_KEY
```

S3 credentials are injected via Kubernetes secrets (`s3-raw-secret`, `s3-master-secret`, `s3-product-secret`).

## Deployment

Built as a Docker image and registered as a Dagster user code deployment. The container starts a gRPC server on port `3030`:

```bash
dagster api grpc -h 0.0.0.0 -p 3030 -m orchestration
```

Dagster daemon and webserver run separately in the homelab cluster and connect to this container via the `dagster-user-deployments` Helm chart.
