
import logging
from datetime import UTC, datetime

import polars as pl
import pytest

from core.storage import WriteResult
from pipelines.raw.candles.config import BinanceCandleSettings
from pipelines.raw.futures.config import FundingRateSettings, FuturesMetricSettings



class MemoryStore:
    def __init__(self) -> None:
        self._tables: dict[str, pl.DataFrame] = {}

    def create_all(self) -> None:
        pass

    def read(self, table: str, *, columns=None) -> pl.DataFrame:
        frame = self._tables.get(table, pl.DataFrame())
        # Skip column selection on empty frames — they have no schema yet.
        if frame.is_empty() or columns is None:
            return frame
        return frame.select(columns)

    def upsert(self, table: str, frame: pl.DataFrame) -> WriteResult:
        self._tables[table] = frame
        return WriteResult(rows_inserted=len(frame), rows_updated=0, write_duration_seconds=0.0)

    def append(self, table: str, frame: pl.DataFrame) -> WriteResult:
        existing = self._tables.get(table)
        self._tables[table] = (
            frame if existing is None or existing.is_empty() else pl.concat([existing, frame])
        )
        return WriteResult(rows_inserted=len(frame), rows_updated=0, write_duration_seconds=0.0)



@pytest.fixture
def memory_store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
def logger() -> logging.Logger:
    return logging.getLogger("test")


@pytest.fixture
def window_start() -> datetime:
    return datetime(2024, 6, 1, tzinfo=UTC)  # matches PAST_MS = 1717200000000


@pytest.fixture
def window_end() -> datetime:
    return datetime(2024, 6, 2, tzinfo=UTC)


@pytest.fixture
def candle_settings() -> BinanceCandleSettings:
    return BinanceCandleSettings(interval="1d")


@pytest.fixture
def funding_rate_settings() -> FundingRateSettings:
    return FundingRateSettings()


@pytest.fixture
def futures_metric_settings() -> FuturesMetricSettings:
    return FuturesMetricSettings()



def make_kline(open_time_ms: int = 1717200000000, close_time_ms: int = 1717286399999) -> list:
    return [
        open_time_ms,
        "65000.00",
        "66000.00",
        "64000.00",
        "65500.00",
        "1000.00",
        close_time_ms,
        "65000000.00",
        5000,
        "500.00",
        "32500000.00",
        "0",  # ignored field
    ]


def make_funding_rate_payload(funding_time_ms: int = 1717200000000) -> dict:
    return {
        "symbol": "BTCUSDT",
        "fundingTime": funding_time_ms,
        "fundingRate": "0.0001",
        "markPrice": "65000.00",
    }


def make_open_interest_payload(timestamp_ms: int = 1717200000000) -> dict:
    return {
        "symbol": "BTCUSDT",
        "timestamp": timestamp_ms,
        "sumOpenInterest": "12345.67",
    }


def make_basis_payload(timestamp_ms: int = 1717200000000) -> dict:
    return {
        "pair": "BTCUSDT",
        "timestamp": timestamp_ms,
        "basis": "100.50",
        "basisRate": "0.0015",
    }


def make_premium_index_kline(open_time_ms: int = 1717200000000, close_time_ms: int = 1717286399999) -> list:
    return [open_time_ms, "0.001", "0.002", "0.0005", "0.0012", "0", close_time_ms, "0", 0, "0", "0", "0"]
