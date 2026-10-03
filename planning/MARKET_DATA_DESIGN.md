# Market Data Backend — Design

**Status:** Design for implementation. Supersedes `planning/archive/MARKET_DATA_DESIGN.md`.
**Scope:** everything under `backend/app/market/`, plus the integration points other backend code uses (FastAPI lifespan, watchlist, trades, portfolio valuation, LLM context) and the SSE contract the frontend consumes.

This document is written against the code that already exists in `backend/app/market/` (summarized in `MARKET_DATA_SUMMARY.md`). Most of that code is sound and is kept as is. This design keeps the parts that work, fixes the defects found while reviewing it against the real `massive` SDK, and adds the pieces the rest of the platform needs but that don't exist yet:

| # | Change | Why |
|---|--------|-----|
| 1 | **Fix Massive snapshot parsing** (`last_trade.sip_timestamp`, not `last_trade.timestamp`) | The SDK's `LastTrade` has no `timestamp` attribute. Today every snapshot raises `AttributeError` and is skipped, so with a real `MASSIVE_API_KEY` the cache stays empty. The unit tests pass only because they build snapshots with `MagicMock`. See §7.1. |
| 2 | **Daily change** (`session_open`, `day_change`, `day_change_percent`) | PLAN §10 requires "daily change %" in the watchlist. The existing `change_percent` is tick-to-tick and is ~0.00% on every update. |
| 3 | **`MarketDataService` facade** with reason-based tracking | PLAN §6 streams "all tickers known to the system". A ticker the user still holds must keep streaming after it leaves the watchlist, or portfolio valuation goes stale. |
| 4 | **Ticker normalization/validation** in one place | Massive upper-cases tickers, the simulator does not; `aapl` and `AAPL` would be two tickers in the simulator. |
| 5 | **SSE hardening**: per-call router, heartbeat, empty snapshot on removal | Fixes review item 3.6 (module-level router), keeps proxies from dropping idle connections during 15s Massive polls, and lets the client drop removed tickers. |
| 6 | **Massive backoff + configurable poll interval** (`MASSIVE_POLL_INTERVAL`) | Free tier is 5 req/min; a 429 should slow the poller down rather than hammer the API. Paid tiers want 2–5s. |
| 7 | **`PriceCache` consistency**: `version` read under the lock; `remove()` bumps `version` | Review item 3.4; and without the bump a removal is invisible to SSE until the next price tick. |

All snippets in §2–§9 were run as a working prototype against the current repository: the existing 73 tests plus the new tests in §12 pass (89 total), and `ruff check` / `ruff format --check` are clean.

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Data Model — `models.py`](#2-data-model--modelspy)
3. [Price Cache — `cache.py`](#3-price-cache--cachepy)
4. [Unified Interface — `interface.py`](#4-unified-interface--interfacepy)
5. [Ticker Normalization — `tickers.py`](#5-ticker-normalization--tickerspy)
6. [GBM Simulator — `seed_prices.py`, `simulator.py`](#6-gbm-simulator)
7. [Massive API Client — `massive_client.py`](#7-massive-api-client--massive_clientpy)
8. [Factory — `factory.py`](#8-factory--factorypy)
9. [MarketDataService — `service.py`](#9-marketdataservice--servicepy)
10. [SSE Streaming — `stream.py`](#10-sse-streaming--streampy)
11. [Backend Integration](#11-backend-integration)
12. [Testing](#12-testing)
13. [Failure Modes](#13-failure-modes)
14. [Configuration](#14-configuration)
15. [Implementation Checklist](#15-implementation-checklist)

---

## 1. Architecture

```
                         ┌───────────────────────────────┐
  MASSIVE_API_KEY? ──────▶  create_market_data_source()  │
                         └──────────────┬────────────────┘
                                        │ returns one of
                 ┌──────────────────────┴──────────────────────┐
                 ▼                                             ▼
     SimulatorDataSource                             MassiveDataSource
     (GBMSimulator, 500ms tick,                      (REST snapshot poll,
      asyncio task)                                   15s default, asyncio task
                 │                                    + worker thread for HTTP)
                 └──────────────┬──────────────────────────────┘
                                │ cache.update(ticker, price, ts, session_open)
                                ▼
                    ┌───────────────────────┐
                    │  PriceCache           │  thread-safe, latest price per ticker,
                    │  (version counter)    │  monotonic version for change detection
                    └───────────┬───────────┘
                                │ reads
        ┌───────────────────────┼─────────────────────────────┬─────────────────┐
        ▼                       ▼                             ▼                 ▼
 GET /api/stream/prices   MarketDataService.require_price  portfolio valuation  LLM context
 (SSE, 500ms check)       (trade execution)                 /api/portfolio       /api/chat

 MarketDataService.track()/untrack()  ──▶  source.add_ticker()/remove_ticker()
   (called by watchlist routes, trade execution, LLM actions)
```

### Rules

- **One writer, many readers.** Exactly one data source writes to the cache. Nothing reads prices from a data source directly; everything reads the cache.
- **Push model with decoupled cadences.** The simulator ticks every 0.5s, Massive polls every 15s, SSE checks the cache every 0.5s and only sends when `version` changed. No consumer needs to know which source is active.
- **`MarketDataService` is the only API the rest of the backend touches.** Routes never call `source.add_ticker()` directly; they call `service.track()` so that the watchlist-vs-position bookkeeping lives in one place.
- **Threading model.** Everything runs on the asyncio event loop except the Massive HTTP call, which runs in a worker thread via `asyncio.to_thread`. `PriceCache` uses a `threading.Lock` so it is safe from both. `MarketDataService` uses an `asyncio.Lock` to serialize add/remove against the source.

### File layout

```
backend/app/market/
  __init__.py         # public API re-exports (changed)
  models.py           # PriceUpdate (changed: session_open, day_change*)
  cache.py            # PriceCache (changed: session_open, version under lock)
  interface.py        # MarketDataSource ABC (unchanged)
  tickers.py          # normalize_ticker (new)
  seed_prices.py      # seed prices, GBM params, correlations (unchanged)
  simulator.py        # GBMSimulator + SimulatorDataSource (unchanged)
  massive_client.py   # MassiveDataSource + parse_snapshot (changed)
  factory.py          # create_market_data_source (changed: MASSIVE_POLL_INTERVAL)
  service.py          # MarketDataService, PriceUnavailableError (new)
  stream.py           # SSE router factory (changed)
```

---

## 2. Data Model — `models.py`

`PriceUpdate` is the only type that leaves the market data layer. It is a frozen, slotted dataclass, so it is safe to share across tasks and threads without copying, and every derived field is a property, so `direction` can never disagree with `price`.

The change from today is one new field, `session_open`, and two properties derived from it. `session_open` is the reference price for "daily change":

- **Massive:** the previous trading day's close (`prev_day.close`), which is how every broker defines day change.
- **Simulator:** the price when the ticker started being tracked (the seed price for default tickers). The simulator has no trading day, so "change since the app started" is the honest equivalent and matches how sparklines fill in "since page load".

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    # Reference price for "daily change": previous close (Massive) or the
    # price when the ticker started being tracked (simulator).
    session_open: float | None = None

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change(self) -> float:
        """Absolute change versus the session reference price."""
        ref = self.session_open if self.session_open is not None else self.price
        return round(self.price - ref, 4)

    @property
    def day_change_percent(self) -> float:
        """Percentage change versus the session reference price."""
        ref = self.session_open if self.session_open is not None else self.price
        if ref == 0:
            return 0.0
        return round((self.price - ref) / ref * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "session_open": self.session_open,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

### Example

```python
>>> u = PriceUpdate("AAPL", price=191.20, previous_price=191.15, timestamp=1707580800.5, session_open=190.00)
>>> u.direction, u.change, u.change_percent
('up', 0.05, 0.0262)
>>> u.day_change, u.day_change_percent
(1.2, 0.6316)
>>> u.to_dict()
{'ticker': 'AAPL', 'price': 191.2, 'previous_price': 191.15, 'timestamp': 1707580800.5,
 'change': 0.05, 'change_percent': 0.0262, 'direction': 'up',
 'session_open': 190.0, 'day_change': 1.2, 'day_change_percent': 0.6316}
```

`session_open` defaults to `None`, so every existing call site and test that constructs `PriceUpdate` positionally keeps working.

---

## 3. Price Cache — `cache.py`

The cache holds the latest `PriceUpdate` per ticker. Memory is O(number of tickers); there is no history (the frontend accumulates sparkline history from the stream; portfolio history lives in `portfolio_snapshots`).

Changes from today:

1. `update()` accepts `session_open`. When it is not passed, the previous entry's value is carried forward, and on first sight it defaults to the price itself. This means the **simulator needs no code change** to get daily change: its first write for a ticker is the seed price, which becomes `session_open` and sticks.
2. `timestamp if timestamp is not None` instead of `timestamp or ...` so a literal `0.0` isn't silently replaced.
3. `version` is read under the lock (review item 3.4; matters on free-threaded Python 3.13t+).
4. `remove()` bumps `version` when it actually removed something, so SSE pushes a snapshot without the removed ticker right away instead of waiting for the next tick (which with Massive could be 15s).

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every write

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        session_open: float | None = None,
    ) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        previous_price is the last cached price (or `price` on first sight, so the
        first update is 'flat'). session_open is taken from the argument if given,
        otherwise carried over from the previous entry, otherwise `price`.
        """
        with self._lock:
            ts = timestamp if timestamp is not None else time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if session_open is None:
                session_open = prev.session_open if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                session_open=round(session_open, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache. Bumps the version so SSE re-sends."""
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why a `threading.Lock`, not an `asyncio.Lock`

The Massive HTTP call runs in a real OS thread (`asyncio.to_thread`). An `asyncio.Lock` only coordinates coroutines on one loop and would not protect against that thread. A `threading.Lock` held for a dict lookup and assignment is effectively uncontended at 10–50 tickers and 2 writes/sec.

### Example

```python
cache = PriceCache()
cache.update("AAPL", 190.00)                  # first sight: flat, session_open=190.00
cache.update("AAPL", 190.37)                  # up, change=0.37, day_change=0.37
cache.update("AAPL", 190.10, session_open=188.00)  # Massive passes prev close explicitly
cache.get("AAPL").day_change_percent          # 1.117
cache.version                                 # 3
cache.remove("AAPL"); cache.version           # 4  (SSE will push a snapshot without AAPL)
```

---

## 4. Unified Interface — `interface.py`

Unchanged. Both sources implement it; the rest of the backend only sees it through `MarketDataService`.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Contract details implementations must honour

| Method | Requirement |
|--------|-------------|
| `start(tickers)` | Populate the cache **before returning** when possible (simulator: seed prices; Massive: one synchronous first poll), so the first SSE frame isn't empty. Then spawn exactly one background task. |
| `stop()` | Cancel and await the task; swallow `CancelledError`; idempotent. Must not write to the cache afterwards. |
| `add_ticker(t)` | Idempotent. Simulator seeds a price immediately; Massive picks it up on the next poll. Receives an already-normalized symbol from `MarketDataService`. |
| `remove_ticker(t)` | Idempotent. Must also call `cache.remove(t)` so SSE stops sending it. |
| `get_tickers()` | Returns a copy; callers may mutate it. |
| Error handling | The background loop must never die on an exception: log, then continue on the next cycle. |

Why the source writes to the cache instead of returning prices: it decouples timing. SSE, trades and valuation read the cache at their own pace and never block on a network call.

---

## 5. Ticker Normalization — `tickers.py`

New module. Every symbol entering the market data layer goes through `normalize_ticker`, called by `MarketDataService`. Routes call it too, to turn bad input into a 400 before touching the database.

```python
"""Ticker symbol normalization and validation."""

from __future__ import annotations

import re

# 1-10 chars: a leading letter, then letters, digits, '.' or '-' (e.g. BRK.B, RDS-A)
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Upper-case and strip a ticker symbol; raise ValueError if malformed."""
    ticker = raw.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker
```

```python
normalize_ticker(" aapl ")   # 'AAPL'
normalize_ticker("BRK.B")    # 'BRK.B'
normalize_ticker("not a ticker!")  # ValueError: Invalid ticker symbol: 'not a ticker!'
```

The regex validates *format* only. Whether the symbol exists is a data-source question: the simulator will happily simulate any well-formed symbol (with a random seed price in $50–$300 and default GBM params), while Massive returns no snapshot for an unknown symbol, so its price stays `null` (see §13).

---

## 6. GBM Simulator

`seed_prices.py` and `simulator.py` are **unchanged**; they are correct and well tested (98% coverage). This section documents how they work so the rest of the team can tune them.

### 6.1 Seed data — `seed_prices.py`

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

### 6.2 The math

Each tick advances every ticker with the exact solution of geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

- `dt = 0.5 / (252 · 6.5 · 3600) ≈ 8.48e-8`: 500ms as a fraction of a trading year, so `μ` and `σ` are ordinary annualized numbers.
- With `σ = 0.22` (AAPL) a single tick moves the price by about `0.22 · √8.48e-8 ≈ 0.0064%`, about 1.2¢ on $190: sub-cent to a few cents, which looks like a real tape. Over an hour (7,200 ticks) that's about 0.54%, a plausible intraday range.
- `exp()` keeps prices strictly positive.

**Correlated moves.** `Z` is not independent per ticker. The simulator builds a correlation matrix from sector groups (tech 0.6, finance 0.5, everything else and TSLA 0.3), takes its Cholesky factor `L` once, and each tick computes `Z = L · z` from independent normals `z`. This is what makes the whole tech block go green together. The matrix is rebuilt on add/remove; with a constant off-diagonal floor of 0.3 and group values ≤ 0.6 it stays positive-definite for any ticker mix.

**Shock events.** Each ticker has a 0.1% chance per tick of an extra ±2–5% jump. With 10 tickers at 2 ticks/sec that's one event about every 50 seconds somewhere on the board: enough drama for the demo without wrecking the random walk.

### 6.3 Simulator classes — `simulator.py`

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Where:
        S(t)   = current price
        mu     = annualized drift (expected return)
        sigma  = annualized volatility
        dt     = time step as fraction of a trading year
        Z      = correlated standard normal random variable

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Correlation structure:
          - Same tech sector:   0.6
          - Same finance sector: 0.5
          - TSLA with anything: 0.3 (it does its own thing)
          - Cross-sector:       0.3
          - Unknown tickers:    0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### 6.4 Tuning examples

```python
# Calmer market for a screenshot: no shocks
SimulatorDataSource(cache, event_probability=0.0)

# "Demo mode": very lively, a shock every few seconds
SimulatorDataSource(cache, event_probability=0.01)

# Deterministic tests
import random, numpy as np
random.seed(42); np.random.seed(42)
sim = GBMSimulator(["AAPL", "MSFT"])
sim.step()   # same output every run
```

Adding a realistic default for a new symbol is a data change only: add it to `SEED_PRICES`, `TICKER_PARAMS` and (optionally) a `CORRELATION_GROUPS` set.

---

## 7. Massive API Client — `massive_client.py`

Polls `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=A,B,C` once per interval for **all** tracked tickers in a single request (the only way to stay inside the free tier's 5 requests/minute). The `massive` `RESTClient` is synchronous (urllib3), so the call runs in a worker thread.

### 7.1 What the SDK actually returns

Verified against the installed `massive` package (`massive.rest.models.TickerSnapshot.from_dict`):

| Wire field | SDK attribute | Notes |
|------------|---------------|-------|
| `ticker` | `snap.ticker` | |
| `lastTrade.p` | `snap.last_trade.price` | `last_trade` is `None` if absent |
| `lastTrade.t` | `snap.last_trade.sip_timestamp` | **nanoseconds**. There is no `last_trade.timestamp`. |
| `day.c` | `snap.day.close` | today's bar; zeros pre-market |
| `prevDay.c` | `snap.prev_day.close` | previous session close → `session_open` |
| `todaysChangePerc` | `snap.todays_change_percent` | not used; derived from `prev_day.close` instead so both sources compute it the same way |
| `updated` | `snap.updated` | nanoseconds |

Errors: any non-200 raises `massive.exceptions.BadResponse(body)` (401 bad key, 403 plan lacks endpoint, 429 rate limit). urllib3 already retries 5xx/connection errors 3 times inside the client.

The current implementation reads `snap.last_trade.timestamp`, which raises `AttributeError` for every real snapshot:

```
$ uv run python -c "...TickerSnapshot.from_dict({'ticker':'AAPL','lastTrade':{'p':190.5,'t':1707580800000000000}})..."
Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'
cached: None
```

### 7.2 Parsing as a pure function

Parsing moves out of the poll loop into `parse_snapshot()`, a pure function that's trivial to test with real SDK objects. It applies a price fallback chain because snapshots are cleared at midnight ET and repopulate from ~4am: outside market hours `last_trade` or `day` may be missing or zero.

```
price      = last_trade.price  →  day.close  →  prev_day.close   (first value > 0)
timestamp  = last_trade.sip_timestamp  →  snap.updated  →  now()
session_open = prev_day.close  (None → cache carries forward / uses price)
```

### 7.3 Code

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class ParsedSnapshot:
    """The fields FinAlly needs from one Massive TickerSnapshot."""

    ticker: str
    price: float
    timestamp: float | None  # Unix seconds, None if the API gave none
    previous_close: float | None


def _to_seconds(ts: int | float | None) -> float | None:
    """Massive timestamps are ns (trades, `updated`) or ms (aggs). Normalize to seconds."""
    if not isinstance(ts, int | float) or ts <= 0:
        return None
    if ts > 1e17:  # nanoseconds
        return ts / 1e9
    if ts > 1e14:  # microseconds
        return ts / 1e6
    if ts > 1e11:  # milliseconds
        return ts / 1e3
    return float(ts)


def parse_snapshot(snap: Any) -> ParsedSnapshot | None:
    """Extract price/timestamp/previous close from a TickerSnapshot.

    Price fallback chain: last trade -> today's bar close -> previous day close.
    (Snapshots are cleared overnight, so last_trade/day can be missing pre-market.)
    Returns None if no usable price exists.
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    last_trade = getattr(snap, "last_trade", None)
    day = getattr(snap, "day", None)
    prev_day = getattr(snap, "prev_day", None)

    candidates = [
        getattr(last_trade, "price", None),
        getattr(day, "close", None),
        getattr(prev_day, "close", None),
    ]
    price = next((p for p in candidates if isinstance(p, int | float) and p > 0), None)
    if price is None:
        return None

    timestamp = _to_seconds(getattr(last_trade, "sip_timestamp", None)) or _to_seconds(
        getattr(snap, "updated", None)
    )
    previous_close = getattr(prev_day, "close", None)
    if not isinstance(previous_close, int | float) or previous_close <= 0:
        previous_close = None
    return ParsedSnapshot(ticker, float(price), timestamp, previous_close)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all tracked
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min -> poll every 15s (default)
      - Paid tiers: higher limits -> poll every 2-5s
    On failure the interval doubles (capped at MAX_BACKOFF_SECONDS) and resets
    after the next successful poll.
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._current_interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(tickers))

        # Immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._current_interval)
            await self._poll_once()

    async def _poll_once(self) -> bool:
        """One poll cycle: fetch snapshots, update cache. Returns True on success."""
        if not self._tickers or not self._client:
            return True

        try:
            # The Massive RESTClient is synchronous (urllib3), so run it in a
            # worker thread to keep the event loop free.
            snapshots = await asyncio.to_thread(self._fetch_snapshots, list(self._tickers))
        except Exception as e:  # BadResponse (401/403/429/5xx), network errors
            self._current_interval = min(self._current_interval * 2, MAX_BACKOFF_SECONDS)
            logger.error(
                "Massive poll failed (next attempt in %.0fs): %s", self._current_interval, e
            )
            return False

        self._current_interval = self._interval
        tracked = set(self._tickers)
        processed = 0
        for snap in snapshots:
            parsed = parse_snapshot(snap)
            if parsed is None:
                logger.warning("Skipping unusable snapshot for %s", getattr(snap, "ticker", "?"))
                continue
            if parsed.ticker not in tracked:  # removed while the request was in flight
                continue
            self._cache.update(
                ticker=parsed.ticker,
                price=parsed.price,
                timestamp=parsed.timestamp,
                session_open=parsed.previous_close,
            )
            processed += 1

        missing = tracked - {getattr(s, "ticker", None) for s in snapshots}
        if missing:
            logger.debug("Massive returned no data for: %s", ", ".join(sorted(missing)))
        logger.debug("Massive poll: updated %d/%d tickers", processed, len(tracked))
        return True

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
```

### 7.4 Behaviour notes

- **Backoff.** On any failure the next sleep doubles (15 → 30 → 60 → 120s cap) and resets to the configured interval after the next success. On the free tier a 429 means the 5-calls/minute budget is spent; backing off is the only thing that helps.
- **In-flight removal.** `_poll_once` snapshots `self._tickers` before the request and drops results for tickers that were removed while the request was in flight, so a removed ticker can't reappear in the cache.
- **Stale but present.** If polls keep failing, the cache keeps the last known prices and SSE keeps serving them. For a trading demo that's better than blanking the board. `PriceUpdate.timestamp` tells the frontend how old a price is if it wants to grey it out.
- **Repeated identical prices.** When the market is closed every poll returns the same last trade. `cache.update` still runs, so the price shows as `flat` and `version` bumps once per poll. That's correct and cheap.
- **Market hours.** No special handling is needed: outside hours prices simply stop moving, which is what a real terminal shows.

### 7.5 Example: poll by hand

```python
import asyncio, os
from app.market import PriceCache
from app.market.massive_client import MassiveDataSource

async def main():
    cache = PriceCache()
    src = MassiveDataSource(os.environ["MASSIVE_API_KEY"], cache, poll_interval=15)
    await src.start(["AAPL", "MSFT"])          # first poll happens here
    for t, u in cache.get_all().items():
        print(t, u.price, f"{u.day_change_percent:+.2f}%")
    await src.stop()

asyncio.run(main())
# AAPL 227.48 +0.84%
# MSFT 415.10 -0.31%
```

---

## 8. Factory — `factory.py`

Selects the source from the environment, per PLAN §5. New: an optional `MASSIVE_POLL_INTERVAL` (seconds, minimum 1.0) so paid-tier users can poll faster without a code change.

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_MASSIVE_POLL_INTERVAL = 15.0  # Safe for the free tier (5 req/min)


def _poll_interval_from_env() -> float:
    raw = os.environ.get("MASSIVE_POLL_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_MASSIVE_POLL_INTERVAL
    try:
        return max(1.0, float(raw))
    except ValueError:
        logger.warning("Ignoring invalid MASSIVE_POLL_INTERVAL=%r", raw)
        return DEFAULT_MASSIVE_POLL_INTERVAL


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty -> MassiveDataSource (real market data)
    - Otherwise -> SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        interval = _poll_interval_from_env()
        logger.info("Market data source: Massive API (real data), poll every %.1fs", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)

    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

| Environment | Result |
|-------------|--------|
| `MASSIVE_API_KEY` unset or blank | `SimulatorDataSource` (0.5s ticks) |
| `MASSIVE_API_KEY=abc` | `MassiveDataSource`, 15s polls |
| `MASSIVE_API_KEY=abc MASSIVE_POLL_INTERVAL=3` | `MassiveDataSource`, 3s polls |
| `MASSIVE_POLL_INTERVAL=fast` | warning logged, 15s polls |

---

## 9. MarketDataService — `service.py`

New. This is the **unified market data API** for the rest of the backend: one object, created at startup, stored on `app.state`, injected into routes. It owns the cache and the source and answers two questions the bare interface can't:

1. **Which tickers should be streaming?** The union of the watchlist and open positions. Each ticker carries a set of *reasons* (`"watchlist"`, `"position"`); it's added to the source when its first reason appears and removed when its last reason goes away.
2. **What price should a trade fill at?** `require_price()` returns the cached price or raises `PriceUnavailableError`, which routes turn into a clean 400.

```python
"""MarketDataService: the single entry point the rest of the backend uses."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from typing import Literal

from .cache import PriceCache
from .interface import MarketDataSource
from .models import PriceUpdate
from .tickers import normalize_ticker

logger = logging.getLogger(__name__)

TrackReason = Literal["watchlist", "position"]


class PriceUnavailableError(LookupError):
    """Raised when a price is required but the cache has none for the ticker."""

    def __init__(self, ticker: str) -> None:
        super().__init__(f"Price not yet available for {ticker}")
        self.ticker = ticker


class MarketDataService:
    """Facade over PriceCache + MarketDataSource.

    Tracks *why* each ticker is streamed. A ticker stays in the data source
    while at least one reason holds: it is on the watchlist, or the user holds
    a position in it. That keeps portfolio valuation live after a held ticker
    is removed from the watchlist.
    """

    def __init__(self, cache: PriceCache, source: MarketDataSource) -> None:
        self.cache = cache
        self.source = source
        self._reasons: dict[str, set[TrackReason]] = {}
        self._lock = asyncio.Lock()  # serializes add/remove against the source

    # --- Lifecycle ---

    async def start(self, watchlist: Iterable[str], held: Iterable[str] = ()) -> None:
        for raw in watchlist:
            self._reasons.setdefault(normalize_ticker(raw), set()).add("watchlist")
        for raw in held:
            self._reasons.setdefault(normalize_ticker(raw), set()).add("position")
        await self.source.start(list(self._reasons))
        logger.info("Market data started for %d tickers", len(self._reasons))

    async def stop(self) -> None:
        await self.source.stop()

    # --- Tracking ---

    async def track(self, ticker: str, reason: TrackReason) -> str:
        """Start (or keep) streaming a ticker. Returns the normalized symbol."""
        ticker = normalize_ticker(ticker)
        async with self._lock:
            reasons = self._reasons.setdefault(ticker, set())
            is_new = not reasons
            reasons.add(reason)
            if is_new:
                await self.source.add_ticker(ticker)
        return ticker

    async def untrack(self, ticker: str, reason: TrackReason) -> str:
        """Drop one reason; stop streaming once no reason is left."""
        ticker = normalize_ticker(ticker)
        async with self._lock:
            reasons = self._reasons.get(ticker)
            if reasons is None:
                return ticker
            reasons.discard(reason)
            if not reasons:
                del self._reasons[ticker]
                await self.source.remove_ticker(ticker)
        return ticker

    def tracked(self) -> list[str]:
        return list(self._reasons)

    # --- Reads ---

    def get(self, ticker: str) -> PriceUpdate | None:
        return self.cache.get(normalize_ticker(ticker))

    def get_price(self, ticker: str) -> float | None:
        return self.cache.get_price(normalize_ticker(ticker))

    def require_price(self, ticker: str) -> float:
        """Price for trade execution; raises PriceUnavailableError if unknown."""
        ticker = normalize_ticker(ticker)
        price = self.cache.get_price(ticker)
        if price is None:
            raise PriceUnavailableError(ticker)
        return price

    def snapshot(self) -> dict[str, PriceUpdate]:
        return self.cache.get_all()
```

### Tracking examples

```python
svc = MarketDataService(cache, source)
await svc.start(watchlist=["AAPL", "GOOGL"], held=["NVDA"])   # streams AAPL, GOOGL, NVDA

await svc.track("pypl", "watchlist")      # 'PYPL' → source.add_ticker('PYPL')
await svc.track("GOOGL", "position")      # bought GOOGL; already streaming, no source call
await svc.untrack("GOOGL", "watchlist")   # removed from watchlist; still held → keeps streaming
await svc.untrack("GOOGL", "position")    # sold out → source.remove_ticker('GOOGL'), cache cleared

svc.require_price("AAPL")                 # 190.37
svc.require_price("ZZZZ")                 # PriceUnavailableError: Price not yet available for ZZZZ
```

### Public API (`app/market/__init__.py`)

```python
"""Market data subsystem for FinAlly.

Public API:
    MarketDataService   - Facade the rest of the backend uses (track/untrack/prices)
    PriceUnavailableError - Raised by MarketDataService.require_price
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
    normalize_ticker    - Upper-case + validate a ticker symbol
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .service import MarketDataService, PriceUnavailableError
from .stream import create_stream_router
from .tickers import normalize_ticker

__all__ = [
    "MarketDataService",
    "PriceUnavailableError",
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
]
```

---

## 10. SSE Streaming — `stream.py`

`GET /api/stream/prices` holds a `text/event-stream` response open and pushes the **full** price map whenever the cache `version` changes, checking every 0.5s.

Changes from today:

- **Router created inside the factory** (review item 3.6). The module-level `router` registered `/prices` again on every call.
- **The generator takes an `is_disconnected` callable** instead of the whole `Request`, which makes it testable without a server (§12).
- **Heartbeat.** After 15s with nothing sent it emits an SSE comment `: ping`. Browsers ignore comments; proxies and load balancers (App Runner, Render, nginx) see traffic and don't time the connection out between 15s Massive polls.
- **Every frame is a full snapshot, including `{}`.** The client replaces its state wholesale, so a removed ticker disappears without a separate "removed" event.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

PUSH_INTERVAL = 0.5  # seconds between cache checks
HEARTBEAT_INTERVAL = 15.0  # seconds of silence before a keep-alive comment


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE router bound to a PriceCache.

    A fresh APIRouter per call, so calling this twice (e.g. in tests) never
    registers /prices twice on a shared module-level router.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request.is_disconnected),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


def format_price_event(price_cache: PriceCache) -> str:
    """Serialize the whole cache as one SSE `data:` frame.

    Every frame is a full snapshot (possibly `{}`), so the client can replace its
    state wholesale and removed tickers disappear without a separate event.
    """
    payload = {ticker: u.to_dict() for ticker, u in price_cache.get_all().items()}
    return f"data: {json.dumps(payload)}\n\n"


async def generate_price_events(
    price_cache: PriceCache,
    is_disconnected,
    interval: float = PUSH_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames whenever the cache version changes.

    - First frame is a `retry:` directive (browser reconnect delay).
    - A full snapshot is sent immediately, then on every version change.
    - A `: ping` comment is sent after `heartbeat` seconds of silence so proxies
      don't close the connection (matters with Massive's 15s polling).
    """
    yield "retry: 1000\n\n"

    last_version = -1
    last_sent = time.monotonic()
    try:
        while not await is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                yield format_price_event(price_cache)
                last_sent = time.monotonic()
            if time.monotonic() - last_sent >= heartbeat:
                yield ": ping\n\n"
                last_sent = time.monotonic()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.debug("SSE stream cancelled")
```

### Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.37,"previous_price":190.35,"timestamp":1707580800.5,"change":0.02,"change_percent":0.0105,"direction":"up","session_open":190.0,"day_change":0.37,"day_change_percent":0.1947},"GOOGL":{...}}

data: {"AAPL":{...},"GOOGL":{...}}

: ping

```

Every PLAN §6 field is present: ticker, price, previous price, timestamp, change direction. At 10 tickers a frame is about 2.3 KB; at 2 frames/sec that's under 5 KB/s per client.

### Frontend contract

```typescript
// frontend/src/lib/prices.ts
export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;          // Unix seconds
  change: number;             // vs previous tick
  change_percent: number;     // vs previous tick
  direction: "up" | "down" | "flat";
  session_open: number | null;
  day_change: number;         // vs session_open → watchlist "daily change"
  day_change_percent: number;
}
export type PriceMap = Record<string, PriceUpdate>;

export function subscribePrices(
  onPrices: (prices: PriceMap) => void,
  onStatus: (s: "connected" | "reconnecting" | "disconnected") => void,
): () => void {
  const es = new EventSource("/api/stream/prices");
  es.onopen = () => onStatus("connected");
  es.onmessage = (e) => onPrices(JSON.parse(e.data) as PriceMap);
  es.onerror = () =>
    onStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
  return () => es.close();
}
```

- **Flash animation:** trigger on `direction !== "flat"` for each ticker whose `timestamp` changed since the last frame.
- **Sparklines:** append `{time: timestamp, value: price}` per ticker per frame; this is the "accumulated since page load" history PLAN §2 describes.
- **Connection dot:** green on `onopen`, yellow while `readyState === CONNECTING`, red on `CLOSED`. The `retry: 1000` directive makes the browser reconnect after 1s.

---

## 11. Backend Integration

These snippets show how the rest of the backend uses the market layer. Database helpers (`db.*`) are placeholders for whatever the backend agent builds in `backend/db/`.

### 11.1 Lifespan — `backend/app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app import db
from app.market import (
    MarketDataService,
    PriceCache,
    create_market_data_source,
    create_stream_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init()                                   # lazy schema + seed (PLAN §7)

    cache = PriceCache()
    service = MarketDataService(cache, create_market_data_source(cache))
    await service.start(
        watchlist=await db.get_watchlist_tickers(),   # 10 defaults on a fresh DB
        held=await db.get_position_tickers(),         # quantity > 0
    )
    app.state.market = service
    app.include_router(create_stream_router(cache))

    snapshot_task = start_snapshot_task(app)          # 30s portfolio_snapshots, PLAN §7
    try:
        yield
    finally:
        snapshot_task.cancel()
        await service.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_market(request: Request) -> MarketDataService:
    return request.app.state.market
```

Including the SSE router inside the lifespan is fine for a single app instance. If the static-files mount (`/`) is added at import time, add the SSE router before it, or create the cache at import time and include the router at module level, so the catch-all mount doesn't shadow `/api/stream/prices`.

### 11.2 Watchlist routes

```python
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.market import MarketDataService, normalize_ticker

router = APIRouter(prefix="/api/watchlist")


class WatchlistAdd(BaseModel):
    ticker: str


@router.get("")
async def list_watchlist(market: MarketDataService = Depends(get_market)):
    rows = await db.get_watchlist_tickers()
    return [
        {"ticker": t, **(u.to_dict() if (u := market.get(t)) else {"price": None})}
        for t in rows
    ]


@router.post("", status_code=201)
async def add_ticker(body: WatchlistAdd, market: MarketDataService = Depends(get_market)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(400, str(e))
    await db.add_watchlist(ticker)                    # INSERT OR IGNORE
    await market.track(ticker, "watchlist")
    u = market.get(ticker)                            # simulator: priced now; Massive: next poll
    return {"ticker": ticker, "price": u.price if u else None}


@router.delete("/{ticker}")
async def remove_ticker(ticker: str, market: MarketDataService = Depends(get_market)):
    ticker = normalize_ticker(ticker)
    await db.remove_watchlist(ticker)
    await market.untrack(ticker, "watchlist")         # keeps streaming if still held
    return {"ticker": ticker}
```

### 11.3 Trade execution

```python
from app.market import PriceUnavailableError


async def execute_trade(market: MarketDataService, ticker: str, side: str, qty: float) -> dict:
    """Shared by POST /api/portfolio/trade and the LLM chat flow."""
    ticker = normalize_ticker(ticker)
    try:
        price = market.require_price(ticker)          # fill at the cached price
    except PriceUnavailableError as e:
        raise TradeError(str(e))

    position = await db.apply_trade(ticker, side, qty, price)   # validates cash/shares
    if position.quantity > 0:
        await market.track(ticker, "position")
    else:
        await market.untrack(ticker, "position")
    await db.record_snapshot(total_value=await portfolio_value(market))
    return {"ticker": ticker, "side": side, "quantity": qty, "price": price}
```

Buying a ticker that isn't on the watchlist works when it's already priced (e.g. it was just added). For a never-seen ticker, `require_price` fails cleanly; the frontend's trade bar only offers tickers it has prices for, and the LLM gets the error text back per PLAN §9.

### 11.4 Portfolio valuation and LLM context

```python
async def portfolio_value(market: MarketDataService) -> float:
    cash = await db.get_cash()
    total = cash
    for p in await db.get_positions():
        price = market.get_price(p.ticker) or p.avg_cost   # fall back to cost if unpriced
        total += p.quantity * price
    return round(total, 2)


def price_context(market: MarketDataService) -> str:
    """Compact block for the LLM system prompt."""
    return "\n".join(
        f"{t}: ${u.price:.2f} ({u.day_change_percent:+.2f}% today)"
        for t, u in sorted(market.snapshot().items())
    )
```

LLM `watchlist_changes` go through the same `market.track/untrack` calls as §11.2, and LLM trades through `execute_trade`.

---

## 12. Testing

Existing suites stay. One existing helper changes, and four suites are new. Run with `uv run --extra dev pytest`.

### 12.1 Fix the Massive test fixture

`tests/market/test_massive.py` builds snapshots with `MagicMock`, which accepts any attribute, so it never noticed the `timestamp` bug. Replace the helper with a real SDK object built from the wire shape:

```python
from massive.rest.models import TickerSnapshot


def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> TickerSnapshot:
    """Build a real TickerSnapshot from the API's wire shape (lastTrade.t is in ns)."""
    return TickerSnapshot.from_dict(
        {"ticker": ticker, "lastTrade": {"p": price, "s": 100, "t": timestamp_ms * 1_000_000}}
    )
```

All existing assertions (including `timestamp == 1707580800.0`) then pass unchanged against the fixed client.

### 12.2 New: `tests/market/test_massive_parsing.py`

```python
"""parse_snapshot against real massive TickerSnapshot objects."""

from massive.rest.models import TickerSnapshot

from app.market.massive_client import _to_seconds, parse_snapshot

NS = 1_707_580_800_000_000_000  # 2024-02-10 16:00:00 UTC in nanoseconds


def snap(**wire) -> TickerSnapshot:
    return TickerSnapshot.from_dict({"ticker": "AAPL", **wire})


def test_last_trade_price_and_ns_timestamp():
    p = parse_snapshot(snap(lastTrade={"p": 190.5, "t": NS}, prevDay={"c": 188.0}))
    assert p.price == 190.5
    assert p.timestamp == 1_707_580_800.0
    assert p.previous_close == 188.0


def test_falls_back_to_day_close_then_prev_close():
    assert parse_snapshot(snap(day={"c": 189.0}, prevDay={"c": 188.0})).price == 189.0
    assert parse_snapshot(snap(prevDay={"c": 188.0})).price == 188.0


def test_zero_day_close_is_ignored():
    # Pre-market the day bar exists but is all zeros
    p = parse_snapshot(snap(day={"c": 0}, prevDay={"c": 188.0}))
    assert p.price == 188.0


def test_no_price_returns_none():
    assert parse_snapshot(snap()) is None


def test_uses_updated_when_no_trade_timestamp():
    p = parse_snapshot(snap(day={"c": 189.0}, updated=NS))
    assert p.timestamp == 1_707_580_800.0


def test_to_seconds_units():
    assert _to_seconds(NS) == 1_707_580_800.0
    assert _to_seconds(1_707_580_800_000) == 1_707_580_800.0
    assert _to_seconds(1_707_580_800) == 1_707_580_800.0
    assert _to_seconds(None) is None
```

### 12.3 New: `tests/market/test_service.py`

```python
"""MarketDataService tracking semantics."""

import pytest

from app.market import MarketDataService, PriceCache, PriceUnavailableError
from app.market.simulator import SimulatorDataSource


@pytest.fixture
async def service():
    cache = PriceCache()
    svc = MarketDataService(cache, SimulatorDataSource(cache, update_interval=60))
    await svc.start(watchlist=["aapl", "GOOGL"], held=["NVDA"])
    yield svc
    await svc.stop()


async def test_start_tracks_watchlist_and_positions(service):
    assert set(service.tracked()) == {"AAPL", "GOOGL", "NVDA"}
    assert service.get_price("aapl") is not None


async def test_untrack_watchlist_keeps_held_ticker(service):
    await service.track("GOOGL", "position")
    await service.untrack("GOOGL", "watchlist")
    assert "GOOGL" in service.tracked()
    assert service.get_price("GOOGL") is not None

    await service.untrack("GOOGL", "position")
    assert "GOOGL" not in service.tracked()
    assert service.get_price("GOOGL") is None


async def test_track_new_ticker_seeds_price(service):
    assert await service.track(" pypl ", "watchlist") == "PYPL"
    assert service.require_price("PYPL") > 0


async def test_require_price_raises_for_unknown(service):
    with pytest.raises(PriceUnavailableError):
        service.require_price("ZZZZ")


async def test_invalid_ticker_rejected(service):
    with pytest.raises(ValueError):
        await service.track("not a ticker!", "watchlist")
```

### 12.4 New: `tests/market/test_stream.py`

Tests the generator directly with a fake `is_disconnected`, so no ASGI server or httpx client is needed (today `stream.py` is at 31% coverage).

```python
"""SSE generator tests (no server needed)."""

import json

from app.market.cache import PriceCache
from app.market.stream import create_stream_router, generate_price_events


def disconnect_after(n: int):
    calls = {"n": 0}

    async def is_disconnected() -> bool:
        calls["n"] += 1
        return calls["n"] > n

    return is_disconnected


async def collect(cache, polls, **kw):
    return [f async for f in generate_price_events(cache, disconnect_after(polls), **kw)]


async def test_retry_then_snapshot():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = await collect(cache, 1, interval=0)
    assert frames[0] == "retry: 1000\n\n"
    data = json.loads(frames[1].removeprefix("data: "))
    assert data["AAPL"]["price"] == 190.0
    assert data["AAPL"]["direction"] == "flat"


async def test_no_resend_without_change():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = await collect(cache, 3, interval=0)
    assert sum(f.startswith("data:") for f in frames) == 1


async def test_heartbeat_when_idle():
    cache = PriceCache()
    frames = await collect(cache, 2, interval=0, heartbeat=0)
    assert ": ping\n\n" in frames


async def test_removal_sends_empty_snapshot():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    cache.remove("AAPL")
    frames = await collect(cache, 1, interval=0)
    assert frames[1] == "data: {}\n\n"


def test_router_factory_is_independent():
    a = create_stream_router(PriceCache())
    b = create_stream_router(PriceCache())
    assert len(a.routes) == len(b.routes) == 1
```

### 12.5 Model and cache additions

Add to the existing suites:

```python
def test_day_change_uses_session_open():
    u = PriceUpdate("AAPL", 191.2, 191.15, 0.0, session_open=190.0)
    assert u.day_change == 1.2
    assert u.day_change_percent == 0.6316


def test_session_open_carries_forward():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    assert cache.update("AAPL", 192.0).session_open == 190.0
    assert cache.update("AAPL", 192.0, session_open=188.0).session_open == 188.0


def test_remove_bumps_version():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    v = cache.version
    cache.remove("AAPL")
    assert cache.version == v + 1
    cache.remove("AAPL")                # no-op, no bump
    assert cache.version == v + 1
```

### 12.6 E2E (Playwright, `test/`)

The simulator is the default, so E2E runs need no key. Useful market-data assertions:

- Within 2s of load, all 10 default tickers show a price.
- A price cell gains then loses the flash class within ~1s.
- Adding `PYPL` shows a price immediately (simulator seeds on add).
- Killing the stream (`page.route('/api/stream/prices', r => r.abort())`) turns the dot yellow/red; restoring it turns it green.

---

## 13. Failure Modes

| Situation | Behaviour | User sees |
|-----------|-----------|-----------|
| No `MASSIVE_API_KEY` | Simulator | Live simulated prices |
| Bad key (401) / plan lacks endpoint (403) | Poll fails, logged, backoff to 120s, keeps retrying | Board empty (fresh start) or frozen; connection dot stays green because SSE itself is fine. Fix `.env`, restart. |
| Rate limited (429) | Backoff doubles up to 120s, resets on success | Prices update less often for a while |
| Network blip | urllib3 retries 3×, then backoff | Prices pause, then resume |
| Unknown symbol on Massive | No snapshot returned, logged at debug | Watchlist row with price `null` ("—") |
| Pre-market / overnight | Falls back to `day.close` → `prev_day.close` | Last close, flat |
| Snapshot missing every price field | Skipped with a warning; others still processed | That ticker keeps its last price |
| Trade on unpriced ticker | `PriceUnavailableError` → 400 | "Price not yet available for X" |
| Ticker removed from watchlist but held | Stays tracked via `"position"` reason | Position keeps valuing live |
| Empty watchlist and no positions | Sources idle; SSE sends `{}` | Empty board; adding a ticker starts it |
| Simulator step throws | Logged, loop continues next tick | Nothing |
| Client disconnects | `is_disconnected()` ends the generator | — |
| Proxy idle timeout | `: ping` every 15s of silence | — |

---

## 14. Configuration

| Setting | Where | Default | Notes |
|---------|-------|---------|-------|
| `MASSIVE_API_KEY` | env | empty | Non-empty → Massive; else simulator |
| `MASSIVE_POLL_INTERVAL` | env | `15` | Seconds, min 1. Free tier: keep ≥ 12. Paid: 2–5 |
| `MAX_BACKOFF_SECONDS` | `massive_client.py` | `120` | Cap on failure backoff |
| `update_interval` | `SimulatorDataSource()` | `0.5` | Simulator tick |
| `event_probability` | `SimulatorDataSource()` | `0.001` | Shock chance per ticker per tick |
| `dt` | `GBMSimulator()` | `≈8.48e-8` | 0.5s in trading-year units |
| `PUSH_INTERVAL` | `stream.py` | `0.5` | SSE cache check cadence |
| `HEARTBEAT_INTERVAL` | `stream.py` | `15` | Silence before `: ping` |
| `retry:` | `stream.py` | `1000` ms | Browser reconnect delay |

Add `MASSIVE_POLL_INTERVAL=` (commented, optional) to `.env.example`.

---

## 15. Implementation Checklist

In order; each step leaves the suite green.

1. `models.py`: add `session_open`, `day_change`, `day_change_percent`, extend `to_dict()`. Add model tests (§12.5).
2. `cache.py`: `session_open` param + carry-forward, `timestamp is not None`, locked `version`, version bump on `remove()`. Add cache tests.
3. `massive_client.py`: add `ParsedSnapshot`, `_to_seconds`, `parse_snapshot`; rewrite `_poll_once` with backoff and in-flight-removal guard; `_fetch_snapshots(tickers)`. Switch the test helper to real `TickerSnapshot` (§12.1); add `test_massive_parsing.py`.
4. `factory.py`: `MASSIVE_POLL_INTERVAL`. Add a factory test for the env var.
5. `tickers.py` and `service.py`: new modules; export from `__init__.py`. Add `test_service.py`.
6. `stream.py`: per-call router, `generate_price_events(cache, is_disconnected)`, heartbeat, always-send snapshot. Add `test_stream.py`.
7. Update `backend/CLAUDE.md` (Market Data API section) and `planning/MARKET_DATA_SUMMARY.md` to point downstream code at `MarketDataService`.
8. Backend agent: wire §11 into `main.py`, watchlist, portfolio and chat routes.
