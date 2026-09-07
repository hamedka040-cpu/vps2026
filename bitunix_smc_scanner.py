# -*- coding: utf-8 -*-
"""
Bitunix Futures 1H SMC+SQZ+RSI+WT+ENG Scanner  (Direct Bitunix REST API - no CCXT)

- Scans all USDT perpetual futures markets on Bitunix via the official REST API
      https://fapi.bitunix.com/api/v1/futures/market/tickers
      https://fapi.bitunix.com/api/v1/futures/market/kline
- Sends Telegram BUY/SELL signals on closed 1H candles
- Implements the 4 confirmation filters:
  1) Valid active OB touch in last candles
  2) Saturation score / active OB-OS
  3) At least one scored divergence
  4) BTC same-direction score

Install:
    pip install -U pandas numpy aiohttp

Run (Linux/Mac):
    export TELEGRAM_BOT_TOKEN=xxxx
    export TELEGRAM_CHAT_ID=xxxx
    python bitunix_smc_scanner.py

Run (Windows CMD):
    set TELEGRAM_BOT_TOKEN=xxxx
    set TELEGRAM_CHAT_ID=xxxx
    python bitunix_smc_scanner.py

Optional ENV:
    TIMEFRAME=1h            SYMBOLS=BTCUSDT,ETHUSDT      MAX_SYMBOLS=50
    DRY_RUN=1  (no telegram, print only)     POLL_SECONDS=60
"""

import asyncio
import html
import json
import math
import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

BULLISH_LEG = 1
BEARISH_LEG = 0
BULLISH = 1
BEARISH = -1


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() not in ("0", "false", "no", "off", "")


@dataclass
class Config:
    # ---------------- Telegram ----------------
    # توکن را هاردکد نکن؛ از ENV بخوان (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    dry_run: bool = _env_bool("DRY_RUN", False)

    # ---------------- Exchange / Scanner ----------------
    timeframe: str = os.getenv("TIMEFRAME", "1h").strip()
    btc_timeframe: str = os.getenv("BTC_TIMEFRAME", "1h").strip()
    btc_symbol: str = "BTCUSDT"

    ohlcv_limit: int = int(os.getenv("OHLCV_LIMIT", "700"))
    min_bars: int = 250
    poll_seconds: int = int(os.getenv("POLL_SECONDS", "60"))
    max_concurrent_requests: int = int(os.getenv("MAX_CONCURRENT", "5"))
    sent_store_file: str = "sent_signals_bitunix.json"

    # اگر خواستی فقط چند ارز خاص اسکن شود: SYMBOLS=BTCUSDT,ETHUSDT
    symbols_whitelist: List[str] = field(
        default_factory=lambda: [s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]
    )
    symbols_blacklist: List[str] = field(default_factory=list)
    max_symbols: int = int(os.getenv("MAX_SYMBOLS", "0"))

    # ---------------- SMC / OB ----------------
    swings_length: int = 50
    internal_structure_length: int = 5
    internal_filter_confluence: bool = False

    show_internal_order_blocks: bool = True
    show_swing_order_blocks: bool = False

    # "Atr" یا "Cumulative Mean Range"
    order_block_filter: str = "Atr"

    # "High/Low" یا "Close"
    order_block_mitigation: str = "High/Low"

    # ---------------- Strategy Settings ----------------
    score_validity_bars: int = 4
    tp_percent: float = 1.0
    ob_entry_score: int = 2
    required_entry_score: int = int(os.getenv("REQUIRED_ENTRY_SCORE", "7"))

    long_orange_arrow_score: int = 2
    long_bull_div_score: int = 5
    short_orange_arrow_score: int = 2
    short_bear_div_score: int = 5

    long_rsi_div_score: int = 5
    long_rsi_os_score: int = 2
    short_rsi_div_score: int = 5
    short_rsi_ob_score: int = 2

    long_wt_div_score: int = 5
    long_wt_os_score: int = 2
    short_wt_div_score: int = 5
    short_wt_ob_score: int = 2

    long_eng_score: int = 0
    short_eng_score: int = 0

    # ---------------- 4 Confirmation Filters ----------------
    # پیام اتصال/شروع به ربات تلگرام + پیام ابتدای هر اسکن
    send_startup_message: bool = _env_bool("SEND_STARTUP", True)
    send_scan_start_message: bool = _env_bool("SEND_SCAN_START", True)

    use_zone_touch_filter: bool = _env_bool("USE_ZONE_TOUCH_FILTER", True)
    use_saturation_score_filter: bool = _env_bool("USE_SATURATION_FILTER", True)
    use_divergence_score_filter: bool = _env_bool("USE_DIVERGENCE_FILTER", True)
    use_btc_confirm_filter: bool = _env_bool("USE_BTC_CONFIRM_FILTER", True)

    filter_lookback_bars: int = 5

    # ---------------- Squeeze Momentum ----------------
    sqz_length: int = 20
    sqz_mult: float = 2.0
    sqz_length_kc: int = 20
    sqz_mult_kc: float = 1.5
    sqz_use_true_range: bool = True
    sqz_lb_r: int = 1
    sqz_lb_l: int = 5
    sqz_max_bars: int = 60

    # ---------------- RSI ----------------
    rsi_length: int = 14
    ob_level: float = 70.0
    os_level: float = 30.0
    show_div: bool = True
    lookback_left: int = 5
    lookback_right: int = 1
    max_pivot_distance: int = 60

    # ---------------- WaveTrend ----------------
    wt_n1: int = 10
    wt_n2: int = 21
    wt_ob_level1: float = 60.0
    wt_ob_level2: float = 53.0
    wt_os_level1: float = -60.0
    wt_os_level2: float = -53.0
    wt_lb_r: int = 1
    wt_lb_l: int = 5
    wt_range_upper: int = 60
    wt_range_lower: int = 5

    # ---------------- Engulfing ----------------
    eng_trend_bars: int = 5


CFG = Config()


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PivotState:
    current_level: float = math.nan
    last_level: float = math.nan
    crossed: bool = False
    bar_time: int = 0
    bar_index: int = 0


@dataclass
class TrendState:
    bias: int = 0


@dataclass
class OrderBlock:
    bar_high: float
    bar_low: float
    bar_time: int
    bias: int
    creation_bar: int
    kind: str = ""  # internal / swing


@dataclass
class SMCResult:
    inside_bullish_ob: np.ndarray
    inside_bearish_ob: np.ndarray
    touched_bullish_recent: np.ndarray
    touched_bearish_recent: np.ndarray
    active_sl_long: np.ndarray
    active_sl_short: np.ndarray
    final_touched_bullish_ob: Optional[OrderBlock]
    final_touched_bearish_ob: Optional[OrderBlock]
    final_internal_obs: List[OrderBlock]
    final_swing_obs: List[OrderBlock]


@dataclass
class SignalResult:
    symbol: str
    side: str  # BUY / SELL
    timestamp: int
    timeframe: str
    close: float
    tp: float
    sl: float
    long_score: int
    short_score: int
    components: Dict[str, int]
    filters: Dict[str, bool]


# =============================================================================
# BASIC UTILS / INDICATORS
# =============================================================================

def is_finite(x: Any) -> bool:
    try:
        return x is not None and np.isfinite(float(x))
    except Exception:
        return False


def timeframe_to_ms(tf: str) -> int:
    tf = tf.strip().lower()
    value = int(tf[:-1])
    unit = tf[-1]
    if unit == "m":
        return value * 60_000
    if unit == "h":
        return value * 60 * 60_000
    if unit == "d":
        return value * 24 * 60 * 60_000
    if unit == "w":
        return value * 7 * 24 * 60 * 60_000
    raise ValueError(f"Unsupported timeframe: {tf}")


def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_price(x: float) -> str:
    if not is_finite(x):
        return "na"
    s = f"{float(x):.10f}"
    s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def sma(x: np.ndarray, length: int) -> np.ndarray:
    return pd.Series(x, dtype="float64").rolling(length, min_periods=length).mean().to_numpy()


def rolling_max(x: np.ndarray, length: int) -> np.ndarray:
    return pd.Series(x, dtype="float64").rolling(length, min_periods=length).max().to_numpy()


def rolling_min(x: np.ndarray, length: int) -> np.ndarray:
    return pd.Series(x, dtype="float64").rolling(length, min_periods=length).min().to_numpy()


def stdev(x: np.ndarray, length: int) -> np.ndarray:
    return pd.Series(x, dtype="float64").rolling(length, min_periods=length).std(ddof=0).to_numpy()


def ema(x: np.ndarray, length: int) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    out = np.full(len(x), np.nan, dtype=float)
    if length <= 0:
        return out
    alpha = 2.0 / (length + 1.0)
    prev = math.nan
    for i, v in enumerate(x):
        if not np.isfinite(v):
            out[i] = prev
            continue
        if not np.isfinite(prev):
            prev = v
        else:
            prev = alpha * v + (1.0 - alpha) * prev
        out[i] = prev
    return out


def rma(x: np.ndarray, length: int) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    out = np.full(len(x), np.nan, dtype=float)
    vals: List[float] = []
    prev = math.nan
    seeded = False

    for i, v in enumerate(x):
        if not np.isfinite(v):
            out[i] = prev if seeded else math.nan
            continue

        if not seeded:
            vals.append(float(v))
            if len(vals) == length:
                prev = float(np.mean(vals))
                out[i] = prev
                seeded = True
            else:
                out[i] = math.nan
        else:
            prev = (prev * (length - 1) + float(v)) / length
            out[i] = prev

    return out


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    n = len(close)
    out = np.full(n, np.nan, dtype=float)
    if n == 0:
        return out
    out[0] = high[0] - low[0]
    for i in range(1, n):
        out[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )
    return out


def linreg(src: np.ndarray, length: int, offset: int = 0) -> np.ndarray:
    src = np.asarray(src, dtype=float)
    n = len(src)
    out = np.full(n, np.nan, dtype=float)
    if length <= 1:
        return src.copy()

    x = np.arange(length, dtype=float)
    x_mean = float(np.mean(x))
    denom = float(np.sum((x - x_mean) ** 2))
    if denom == 0:
        return out

    for i in range(length - 1, n):
        y = src[i - length + 1:i + 1]
        if not np.all(np.isfinite(y)):
            continue
        y_mean = float(np.mean(y))
        slope = float(np.sum((x - x_mean) * (y - y_mean)) / denom)
        intercept = y_mean - slope * x_mean
        out[i] = intercept + slope * (length - 1 - offset)

    return out


def crossover_const(x: np.ndarray, level: float) -> np.ndarray:
    out = np.zeros(len(x), dtype=bool)
    for i in range(1, len(x)):
        if np.isfinite(x[i]) and np.isfinite(x[i - 1]):
            out[i] = x[i] > level and x[i - 1] <= level
    return out


def crossunder_const(x: np.ndarray, level: float) -> np.ndarray:
    out = np.zeros(len(x), dtype=bool)
    for i in range(1, len(x)):
        if np.isfinite(x[i]) and np.isfinite(x[i - 1]):
            out[i] = x[i] < level and x[i - 1] >= level
    return out


def is_pivot_low_at(values: np.ndarray, p: int, left: int, right: int) -> bool:
    if p - left < 0 or p + right >= len(values):
        return False
    c = values[p]
    if not np.isfinite(c):
        return False
    lv = values[p - left:p]
    rv = values[p + 1:p + right + 1]
    if not np.all(np.isfinite(lv)) or not np.all(np.isfinite(rv)):
        return False
    return bool(np.all(c < lv) and np.all(c <= rv))


def is_pivot_high_at(values: np.ndarray, p: int, left: int, right: int) -> bool:
    if p - left < 0 or p + right >= len(values):
        return False
    c = values[p]
    if not np.isfinite(c):
        return False
    lv = values[p - left:p]
    rv = values[p + 1:p + right + 1]
    if not np.all(np.isfinite(lv)) or not np.all(np.isfinite(rv)):
        return False
    return bool(np.all(c > lv) and np.all(c >= rv))


def recent_score_at(cond: np.ndarray, score: int, window: int, i: int) -> int:
    if score <= 0:
        return 0
    window = max(0, int(window))
    start = max(0, i - window)
    for j in range(i, start - 1, -1):
        if bool(cond[j]):
            return int(score)
    return 0


# =============================================================================
# MAIN INDICATORS
# =============================================================================

def compute_indicators(df: pd.DataFrame, cfg: Config) -> Dict[str, np.ndarray]:
    o = df["open"].to_numpy(dtype=float)
    h = df["high"].to_numpy(dtype=float)
    l = df["low"].to_numpy(dtype=float)
    c = df["close"].to_numpy(dtype=float)
    n = len(df)

    tr = true_range(h, l, c)
    atr14 = rma(tr, 14)
    atr200 = rma(tr, 200)

    # ---------------- Squeeze Momentum ----------------
    highest_h = rolling_max(h, cfg.sqz_length_kc)
    lowest_l = rolling_min(l, cfg.sqz_length_kc)
    sma_close_kc = sma(c, cfg.sqz_length_kc)

    sqz_mid = ((highest_h + lowest_l) / 2.0 + sma_close_kc) / 2.0
    sqz_src = c - sqz_mid
    sqz_val = linreg(sqz_src, cfg.sqz_length_kc, 0)

    prev_sqz = np.roll(sqz_val, 1)
    prev_sqz[0] = 0.0
    prev_sqz = np.nan_to_num(prev_sqz, nan=0.0)

    sqz_is_dark_green = (sqz_val > 0) & (sqz_val < prev_sqz)
    sqz_is_maroon = (sqz_val < 0) & (sqz_val > prev_sqz)

    sqz_bull_div = np.zeros(n, dtype=bool)
    sqz_bear_div = np.zeros(n, dtype=bool)

    last_pl_bar = None
    last_pl_val = math.nan
    last_pl_low = math.nan

    last_ph_bar = None
    last_ph_val = math.nan
    last_ph_high = math.nan

    for i in range(n):
        p = i - cfg.sqz_lb_r

        if p >= 0 and is_pivot_low_at(sqz_val, p, cfg.sqz_lb_l, cfg.sqz_lb_r):
            if last_pl_bar is not None:
                dist = p - last_pl_bar
                cond = (
                    l[p] < last_pl_low
                    and sqz_val[p] > last_pl_val
                    and sqz_val[p] < 0
                    and dist <= cfg.sqz_max_bars
                )
                sqz_bull_div[i] = bool(cond)
            last_pl_bar = p
            last_pl_val = float(sqz_val[p])
            last_pl_low = float(l[p])

        if p >= 0 and is_pivot_high_at(sqz_val, p, cfg.sqz_lb_l, cfg.sqz_lb_r):
            if last_ph_bar is not None:
                dist = p - last_ph_bar
                cond = (
                    h[p] > last_ph_high
                    and sqz_val[p] < last_ph_val
                    and sqz_val[p] > 0
                    and dist <= cfg.sqz_max_bars
                )
                sqz_bear_div[i] = bool(cond)
            last_ph_bar = p
            last_ph_val = float(sqz_val[p])
            last_ph_high = float(h[p])

    sqz_top_exhaustion = np.zeros(n, dtype=bool)
    sqz_bottom_exhaustion = np.zeros(n, dtype=bool)

    dg_start_bar = None
    dg_start_high = math.nan

    mar_start_bar = None
    mar_start_low = math.nan

    for i in range(n):
        prev_dg = bool(sqz_is_dark_green[i - 1]) if i > 0 else False
        prev_mar = bool(sqz_is_maroon[i - 1]) if i > 0 else False

        if bool(sqz_is_dark_green[i]) and not prev_dg:
            dg_start_bar = i
            dg_start_high = float(h[i])

        if i > 0:
            dg_ended = prev_dg and not bool(sqz_is_dark_green[i])
            if (
                dg_ended
                and dg_start_bar is not None
                and i - 1 > dg_start_bar
                and h[i - 1] > dg_start_high
            ):
                sqz_top_exhaustion[i] = True

        if bool(sqz_is_maroon[i]) and not prev_mar:
            mar_start_bar = i
            mar_start_low = float(l[i])

        if i > 0:
            mar_ended = prev_mar and not bool(sqz_is_maroon[i])
            if (
                mar_ended
                and mar_start_bar is not None
                and i - 1 > mar_start_bar
                and l[i - 1] < mar_start_low
            ):
                sqz_bottom_exhaustion[i] = True

    # ---------------- RSI ----------------
    rsi_change = np.full(n, np.nan, dtype=float)
    if n > 1:
        rsi_change[1:] = np.diff(c)

    rsi_up_raw = np.where(np.isfinite(rsi_change), np.maximum(rsi_change, 0.0), np.nan)
    rsi_down_raw = np.where(np.isfinite(rsi_change), -np.minimum(rsi_change, 0.0), np.nan)

    rsi_up = rma(rsi_up_raw, cfg.rsi_length)
    rsi_down = rma(rsi_down_raw, cfg.rsi_length)

    rsi = np.full(n, np.nan, dtype=float)
    for i in range(n):
        if not np.isfinite(rsi_up[i]) or not np.isfinite(rsi_down[i]):
            continue
        if rsi_down[i] == 0:
            rsi[i] = 100.0
        elif rsi_up[i] == 0:
            rsi[i] = 0.0
        else:
            rsi[i] = 100.0 - 100.0 / (1.0 + rsi_up[i] / rsi_down[i])

    in_ob = np.isfinite(rsi) & (rsi >= cfg.ob_level)
    in_os = np.isfinite(rsi) & (rsi <= cfg.os_level)
    rsi_ob_entry = crossover_const(rsi, cfg.ob_level)
    rsi_os_entry = crossunder_const(rsi, cfg.os_level)

    rsi_bull_div = np.zeros(n, dtype=bool)
    rsi_bear_div = np.zeros(n, dtype=bool)

    last_bull_rsi = math.nan
    last_bull_price = math.nan
    last_bull_bar = None

    last_bear_rsi = math.nan
    last_bear_price = math.nan
    last_bear_bar = None

    if cfg.show_div:
        for i in range(n):
            p = i - cfg.lookback_right

            if p >= 0 and is_pivot_low_at(rsi, p, cfg.lookback_left, cfg.lookback_right):
                curr_bar = p
                curr_price = float(l[p])
                curr_rsi = float(rsi[p])

                if last_bull_bar is not None:
                    dist = curr_bar - last_bull_bar
                    if (
                        curr_rsi > last_bull_rsi
                        and curr_price < last_bull_price
                        and dist >= cfg.lookback_left
                        and dist <= cfg.max_pivot_distance
                    ):
                        rsi_bull_div[i] = True

                last_bull_rsi = curr_rsi
                last_bull_price = curr_price
                last_bull_bar = curr_bar

            if p >= 0 and is_pivot_high_at(rsi, p, cfg.lookback_left, cfg.lookback_right):
                curr_bar = p
                curr_price = float(h[p])
                curr_rsi = float(rsi[p])

                if last_bear_bar is not None:
                    dist = curr_bar - last_bear_bar
                    if (
                        curr_rsi < last_bear_rsi
                        and curr_price > last_bear_price
                        and dist >= cfg.lookback_left
                        and dist <= cfg.max_pivot_distance
                    ):
                        rsi_bear_div[i] = True

                last_bear_rsi = curr_rsi
                last_bear_price = curr_price
                last_bear_bar = curr_bar

    # ---------------- WaveTrend ----------------
    wt_ap = (h + l + c) / 3.0
    wt_esa = ema(wt_ap, cfg.wt_n1)
    wt_d = ema(np.abs(wt_ap - wt_esa), cfg.wt_n1)

    wt_ci = np.full(n, np.nan, dtype=float)
    for i in range(n):
        if not np.isfinite(wt_d[i]) or not np.isfinite(wt_esa[i]):
            continue
        wt_ci[i] = 0.0 if wt_d[i] == 0 else (wt_ap[i] - wt_esa[i]) / (0.015 * wt_d[i])

    wt1 = ema(wt_ci, cfg.wt_n2)

    wt_ob_extreme = np.isfinite(wt1) & (wt1 >= cfg.wt_ob_level1)
    wt_os_extreme = np.isfinite(wt1) & (wt1 <= cfg.wt_os_level1)
    wt_ob_zone = np.isfinite(wt1) & (wt1 >= cfg.wt_ob_level2)
    wt_os_zone = np.isfinite(wt1) & (wt1 <= cfg.wt_os_level2)

    wt_ob_entry = np.zeros(n, dtype=bool)
    wt_os_entry = np.zeros(n, dtype=bool)
    for i in range(1, n):
        wt_ob_entry[i] = bool(wt_ob_extreme[i] and not wt_ob_extreme[i - 1])
        wt_os_entry[i] = bool(wt_os_extreme[i] and not wt_os_extreme[i - 1])

    wt_bull_div = np.zeros(n, dtype=bool)
    wt_bear_div = np.zeros(n, dtype=bool)

    last_wt_pl_bar = None
    last_wt_pl_osc = math.nan
    last_wt_pl_price = math.nan

    last_wt_ph_bar = None
    last_wt_ph_osc = math.nan
    last_wt_ph_price = math.nan

    for i in range(n):
        p = i - cfg.wt_lb_r

        if p >= 0 and is_pivot_low_at(wt1, p, cfg.wt_lb_l, cfg.wt_lb_r):
            if last_wt_pl_bar is not None:
                dist = p - last_wt_pl_bar
                if (
                    dist >= cfg.wt_range_lower
                    and dist <= cfg.wt_range_upper
                    and l[p] < last_wt_pl_price
                    and wt1[p] > last_wt_pl_osc
                ):
                    wt_bull_div[i] = True

            last_wt_pl_bar = p
            last_wt_pl_osc = float(wt1[p])
            last_wt_pl_price = float(l[p])

        if p >= 0 and is_pivot_high_at(wt1, p, cfg.wt_lb_l, cfg.wt_lb_r):
            if last_wt_ph_bar is not None:
                dist = p - last_wt_ph_bar
                if (
                    dist >= cfg.wt_range_lower
                    and dist <= cfg.wt_range_upper
                    and h[p] > last_wt_ph_price
                    and wt1[p] < last_wt_ph_osc
                ):
                    wt_bear_div[i] = True

            last_wt_ph_bar = p
            last_wt_ph_osc = float(wt1[p])
            last_wt_ph_price = float(h[p])

    # ---------------- Engulfing ----------------
    bull_eng = np.zeros(n, dtype=bool)
    bear_eng = np.zeros(n, dtype=bool)

    for i in range(n):
        if i < max(1, cfg.eng_trend_bars):
            continue

        bear_eng[i] = bool(
            c[i - 1] > o[i - 1]
            and o[i] > c[i]
            and o[i] >= c[i - 1]
            and o[i - 1] >= c[i]
            and (o[i] - c[i]) > (c[i - 1] - o[i - 1])
            and o[i - cfg.eng_trend_bars] < o[i]
        )

        bull_eng[i] = bool(
            o[i - 1] > c[i - 1]
            and c[i] > o[i]
            and c[i] >= o[i - 1]
            and c[i - 1] >= o[i]
            and (c[i] - o[i]) > (o[i - 1] - c[i - 1])
            and o[i - cfg.eng_trend_bars] > o[i]
        )

    return {
        "tr": tr,
        "atr14": atr14,
        "atr200": atr200,

        "sqz_val": sqz_val,
        "sqz_top_exhaustion": sqz_top_exhaustion,
        "sqz_bottom_exhaustion": sqz_bottom_exhaustion,
        "sqz_bull_div": sqz_bull_div,
        "sqz_bear_div": sqz_bear_div,

        "rsi": rsi,
        "in_ob": in_ob,
        "in_os": in_os,
        "rsi_ob_entry": rsi_ob_entry,
        "rsi_os_entry": rsi_os_entry,
        "rsi_bull_div": rsi_bull_div,
        "rsi_bear_div": rsi_bear_div,

        "wt1": wt1,
        "wt_ob_extreme": wt_ob_extreme,
        "wt_os_extreme": wt_os_extreme,
        "wt_ob_zone": wt_ob_zone,
        "wt_os_zone": wt_os_zone,
        "wt_ob_entry": wt_ob_entry,
        "wt_os_entry": wt_os_entry,
        "wt_bull_div": wt_bull_div,
        "wt_bear_div": wt_bear_div,

        "bull_eng": bull_eng,
        "bear_eng": bear_eng,
    }


# =============================================================================
# BTC CONFIRMATION
# =============================================================================

def compute_btc_points(ind: Dict[str, np.ndarray], cfg: Config) -> Tuple[np.ndarray, np.ndarray]:
    n = len(ind["sqz_val"])
    btc_long = np.zeros(n, dtype=bool)
    btc_short = np.zeros(n, dtype=bool)

    rsi_os_or_active = np.logical_or(ind["rsi_os_entry"], ind["in_os"])
    rsi_ob_or_active = np.logical_or(ind["rsi_ob_entry"], ind["in_ob"])
    wt_os_or_active = np.logical_or(ind["wt_os_entry"], ind["wt_os_zone"])
    wt_ob_or_active = np.logical_or(ind["wt_ob_entry"], ind["wt_ob_zone"])

    lb = cfg.filter_lookback_bars
    for i in range(n):
        btc_long[i] = bool(
            recent_score_at(ind["sqz_bottom_exhaustion"], cfg.long_orange_arrow_score, lb, i) > 0
            or recent_score_at(ind["sqz_bull_div"], cfg.long_bull_div_score, lb, i) > 0
            or recent_score_at(ind["rsi_bull_div"], cfg.long_rsi_div_score, lb, i) > 0
            or recent_score_at(rsi_os_or_active, cfg.long_rsi_os_score, lb, i) > 0
            or recent_score_at(ind["wt_bull_div"], cfg.long_wt_div_score, lb, i) > 0
            or recent_score_at(wt_os_or_active, cfg.long_wt_os_score, lb, i) > 0
            or recent_score_at(ind["bull_eng"], cfg.long_eng_score, lb, i) > 0
        )

        btc_short[i] = bool(
            recent_score_at(ind["sqz_top_exhaustion"], cfg.short_orange_arrow_score, lb, i) > 0
            or recent_score_at(ind["sqz_bear_div"], cfg.short_bear_div_score, lb, i) > 0
            or recent_score_at(ind["rsi_bear_div"], cfg.short_rsi_div_score, lb, i) > 0
            or recent_score_at(rsi_ob_or_active, cfg.short_rsi_ob_score, lb, i) > 0
            or recent_score_at(ind["wt_bear_div"], cfg.short_wt_div_score, lb, i) > 0
            or recent_score_at(wt_ob_or_active, cfg.short_wt_ob_score, lb, i) > 0
            or recent_score_at(ind["bear_eng"], cfg.short_eng_score, lb, i) > 0
        )

    return btc_long, btc_short


def align_bool_by_timestamp(src_times: np.ndarray, src_values: np.ndarray, dst_times: np.ndarray) -> np.ndarray:
    src_times = np.asarray(src_times, dtype=np.int64)
    src_values = np.asarray(src_values, dtype=bool)
    dst_times = np.asarray(dst_times, dtype=np.int64)

    out = np.zeros(len(dst_times), dtype=bool)
    if len(src_times) == 0:
        return out

    idx = np.searchsorted(src_times, dst_times, side="right") - 1
    mask = idx >= 0
    out[mask] = src_values[idx[mask]]
    return out


# =============================================================================
# SMC ORDER BLOCK ENGINE
# =============================================================================

def run_smc_orderblocks(df: pd.DataFrame, cfg: Config) -> SMCResult:
    o = df["open"].to_numpy(dtype=float)
    h = df["high"].to_numpy(dtype=float)
    l = df["low"].to_numpy(dtype=float)
    c = df["close"].to_numpy(dtype=float)
    t = df["timestamp"].to_numpy(dtype=np.int64)
    n = len(df)

    tr = true_range(h, l, c)
    atr200 = rma(tr, 200)

    if cfg.order_block_filter.lower().startswith("atr"):
        volatility = atr200
    else:
        cum_tr = np.cumsum(np.nan_to_num(tr, nan=0.0))
        denom = np.maximum(np.arange(n), 1)
        volatility = cum_tr / denom

    high_vol_bar = np.where(np.isfinite(volatility), (h - l) >= (2.0 * volatility), False)
    parsed_highs = np.where(high_vol_bar, l, h)
    parsed_lows = np.where(high_vol_bar, h, l)

    use_close_mitigation = cfg.order_block_mitigation.lower().startswith("close")
    bearish_mitigation_source = c if use_close_mitigation else h
    bullish_mitigation_source = c if use_close_mitigation else l

    swing_high = PivotState()
    swing_low = PivotState()
    internal_high = PivotState()
    internal_low = PivotState()

    swing_trend = TrendState()
    internal_trend = TrendState()

    leg_states: Dict[str, int] = {"swing": BEARISH_LEG, "internal": BEARISH_LEG}

    swing_order_blocks: List[OrderBlock] = []
    internal_order_blocks: List[OrderBlock] = []

    inside_bullish_ob = np.zeros(n, dtype=bool)
    inside_bearish_ob = np.zeros(n, dtype=bool)
    touched_bullish_recent = np.zeros(n, dtype=bool)
    touched_bearish_recent = np.zeros(n, dtype=bool)
    active_sl_long = np.full(n, np.nan, dtype=float)
    active_sl_short = np.full(n, np.nan, dtype=float)

    final_touched_bull_ob: Optional[OrderBlock] = None
    final_touched_bear_ob: Optional[OrderBlock] = None

    def get_current_structure(i: int, size: int, key: str, internal: bool) -> None:
        if i < size:
            return

        prev_leg = leg_states[key]
        new_leg = prev_leg

        window_high = h[i - size + 1:i + 1]
        window_low = l[i - size + 1:i + 1]

        new_leg_high = h[i - size] > np.max(window_high)
        new_leg_low = l[i - size] < np.min(window_low)

        if new_leg_high:
            new_leg = BEARISH_LEG
        elif new_leg_low:
            new_leg = BULLISH_LEG

        change = new_leg - prev_leg
        leg_states[key] = new_leg

        if change == 0:
            return

        pivot_low = change == +1

        if pivot_low:
            p = internal_low if internal else swing_low
            p.last_level = p.current_level
            p.current_level = float(l[i - size])
            p.crossed = False
            p.bar_time = int(t[i - size])
            p.bar_index = i - size
        else:
            p = internal_high if internal else swing_high
            p.last_level = p.current_level
            p.current_level = float(h[i - size])
            p.crossed = False
            p.bar_time = int(t[i - size])
            p.bar_index = i - size

    def store_order_block(pivot: PivotState, internal: bool, bias: int, i: int) -> None:
        need_internal = cfg.show_internal_order_blocks or cfg.use_zone_touch_filter
        need_swing = cfg.show_swing_order_blocks or cfg.use_zone_touch_filter

        if internal and not need_internal:
            return
        if (not internal) and not need_swing:
            return

        start = max(0, int(pivot.bar_index))
        end = i

        if end <= start:
            return

        if bias == BEARISH:
            segment = parsed_highs[start:end]
            if len(segment) == 0 or not np.any(np.isfinite(segment)):
                return
            idx = start + int(np.nanargmax(segment))
        else:
            segment = parsed_lows[start:end]
            if len(segment) == 0 or not np.any(np.isfinite(segment)):
                return
            idx = start + int(np.nanargmin(segment))

        ob = OrderBlock(
            bar_high=float(parsed_highs[idx]),
            bar_low=float(parsed_lows[idx]),
            bar_time=int(t[idx]),
            bias=bias,
            creation_bar=i,
            kind="internal" if internal else "swing",
        )

        arr = internal_order_blocks if internal else swing_order_blocks
        if len(arr) >= 100:
            arr.pop()
        arr.insert(0, ob)

    def display_structure(i: int, internal: bool, prev_high_level: float, prev_low_level: float) -> None:
        bullish_bar = True
        bearish_bar = True

        if cfg.internal_filter_confluence:
            upper_wick = h[i] - max(c[i], o[i])
            lower_wick = min(c[i], o[i]) - l[i]
            bullish_bar = upper_wick > lower_wick
            bearish_bar = upper_wick < lower_wick

        trend = internal_trend if internal else swing_trend

        p_high = internal_high if internal else swing_high
        if internal:
            extra_bull = (
                is_finite(internal_high.current_level)
                and is_finite(swing_high.current_level)
                and not math.isclose(internal_high.current_level, swing_high.current_level)
                and bullish_bar
            )
        else:
            extra_bull = True

        bullish_cross = (
            i > 0
            and is_finite(p_high.current_level)
            and is_finite(prev_high_level)
            and c[i] > p_high.current_level
            and c[i - 1] <= prev_high_level
        )

        if bullish_cross and not p_high.crossed and extra_bull:
            p_high.crossed = True
            trend.bias = BULLISH
            store_order_block(p_high, internal, BULLISH, i)

        p_low = internal_low if internal else swing_low
        if internal:
            extra_bear = (
                is_finite(internal_low.current_level)
                and is_finite(swing_low.current_level)
                and not math.isclose(internal_low.current_level, swing_low.current_level)
                and bearish_bar
            )
        else:
            extra_bear = True

        bearish_cross = (
            i > 0
            and is_finite(p_low.current_level)
            and is_finite(prev_low_level)
            and c[i] < p_low.current_level
            and c[i - 1] >= prev_low_level
        )

        if bearish_cross and not p_low.crossed and extra_bear:
            p_low.crossed = True
            trend.bias = BEARISH
            store_order_block(p_low, internal, BEARISH, i)

    def delete_invalid_obs(i: int) -> None:
        def still_valid(ob: OrderBlock) -> bool:
            if ob.bias == BEARISH:
                return not (bearish_mitigation_source[i] > ob.bar_high)
            return not (bullish_mitigation_source[i] < ob.bar_low)

        internal_order_blocks[:] = [ob for ob in internal_order_blocks if still_valid(ob)]
        swing_order_blocks[:] = [ob for ob in swing_order_blocks if still_valid(ob)]

    def touched_recent(order_blocks: List[OrderBlock], bias: int, i: int) -> Tuple[bool, Optional[OrderBlock]]:
        for ob in order_blocks:
            if ob.bias != bias:
                continue
            for off in range(cfg.filter_lookback_bars):
                j = i - off
                if j < 0:
                    break
                if j > ob.creation_bar and h[j] >= ob.bar_low and l[j] <= ob.bar_high:
                    return True, ob
        return False, None

    for i in range(n):
        prev_swing_high = swing_high.current_level
        prev_swing_low = swing_low.current_level
        prev_internal_high = internal_high.current_level
        prev_internal_low = internal_low.current_level

        get_current_structure(i, cfg.swings_length, "swing", internal=False)
        get_current_structure(i, cfg.internal_structure_length, "internal", internal=True)

        display_structure(i, internal=True, prev_high_level=prev_internal_high, prev_low_level=prev_internal_low)
        display_structure(i, internal=False, prev_high_level=prev_swing_high, prev_low_level=prev_swing_low)

        delete_invalid_obs(i)

        found_long_ob: Optional[OrderBlock] = None
        for ob in internal_order_blocks:
            if ob.bias == BULLISH and l[i] <= ob.bar_high and c[i] >= ob.bar_low and i > ob.creation_bar:
                found_long_ob = ob
                break
        if found_long_ob is None:
            for ob in swing_order_blocks:
                if ob.bias == BULLISH and l[i] <= ob.bar_high and c[i] >= ob.bar_low and i > ob.creation_bar:
                    found_long_ob = ob
                    break
        if found_long_ob is not None:
            inside_bullish_ob[i] = True
            active_sl_long[i] = found_long_ob.bar_low

        found_short_ob: Optional[OrderBlock] = None
        for ob in internal_order_blocks:
            if ob.bias == BEARISH and h[i] >= ob.bar_low and c[i] <= ob.bar_high and i > ob.creation_bar:
                found_short_ob = ob
                break
        if found_short_ob is None:
            for ob in swing_order_blocks:
                if ob.bias == BEARISH and h[i] >= ob.bar_low and c[i] <= ob.bar_high and i > ob.creation_bar:
                    found_short_ob = ob
                    break
        if found_short_ob is not None:
            inside_bearish_ob[i] = True
            active_sl_short[i] = found_short_ob.bar_high

        bull_touch_i, bull_ob_i = touched_recent(internal_order_blocks, BULLISH, i)
        if not bull_touch_i:
            bull_touch_i, bull_ob_i = touched_recent(swing_order_blocks, BULLISH, i)

        bear_touch_i, bear_ob_i = touched_recent(internal_order_blocks, BEARISH, i)
        if not bear_touch_i:
            bear_touch_i, bear_ob_i = touched_recent(swing_order_blocks, BEARISH, i)

        touched_bullish_recent[i] = bull_touch_i
        touched_bearish_recent[i] = bear_touch_i

        if i == n - 1:
            final_touched_bull_ob = bull_ob_i
            final_touched_bear_ob = bear_ob_i

    return SMCResult(
        inside_bullish_ob=inside_bullish_ob,
        inside_bearish_ob=inside_bearish_ob,
        touched_bullish_recent=touched_bullish_recent,
        touched_bearish_recent=touched_bearish_recent,
        active_sl_long=active_sl_long,
        active_sl_short=active_sl_short,
        final_touched_bullish_ob=final_touched_bull_ob,
        final_touched_bearish_ob=final_touched_bear_ob,
        final_internal_obs=list(internal_order_blocks),
        final_swing_obs=list(swing_order_blocks),
    )


# =============================================================================
# STRATEGY ANALYSIS
# =============================================================================

def analyze_symbol(
    symbol: str,
    df: pd.DataFrame,
    cfg: Config,
    btc_context: Optional[Tuple[np.ndarray, np.ndarray, np.ndarray]],
) -> Optional[SignalResult]:
    if len(df) < cfg.min_bars:
        return None

    ind = compute_indicators(df, cfg)
    smc = run_smc_orderblocks(df, cfg)

    h = df["high"].to_numpy(dtype=float)
    l = df["low"].to_numpy(dtype=float)
    c = df["close"].to_numpy(dtype=float)
    times = df["timestamp"].to_numpy(dtype=np.int64)
    n = len(df)

    if btc_context is not None:
        btc_times, btc_long_points, btc_short_points = btc_context
        btc_long_aligned = align_bool_by_timestamp(btc_times, btc_long_points, times)
        btc_short_aligned = align_bool_by_timestamp(btc_times, btc_short_points, times)
    else:
        btc_long_aligned = np.zeros(n, dtype=bool)
        btc_short_aligned = np.zeros(n, dtype=bool)

    last_long_entry_bar = -999999
    last_short_entry_bar = -999999

    final_side: Optional[str] = None
    final_long_score = 0
    final_short_score = 0
    final_components: Dict[str, int] = {}
    final_filters: Dict[str, bool] = {}

    sv = cfg.score_validity_bars
    for i in range(n):
        long_components = {
            "SQZ Bottom Exhaustion": recent_score_at(ind["sqz_bottom_exhaustion"], cfg.long_orange_arrow_score, sv, i),
            "SQZ Bullish Divergence": recent_score_at(ind["sqz_bull_div"], cfg.long_bull_div_score, sv, i),
            "RSI Bullish Divergence": recent_score_at(ind["rsi_bull_div"], cfg.long_rsi_div_score, sv, i),
            "RSI Oversold Entry": recent_score_at(ind["rsi_os_entry"], cfg.long_rsi_os_score, sv, i),
            "WT Bullish Divergence": recent_score_at(ind["wt_bull_div"], cfg.long_wt_div_score, sv, i),
            "WT Oversold Entry": recent_score_at(ind["wt_os_entry"], cfg.long_wt_os_score, sv, i),
            "Bullish Engulfing": recent_score_at(ind["bull_eng"], cfg.long_eng_score, sv, i),
            "Bullish OB": recent_score_at(smc.inside_bullish_ob, cfg.ob_entry_score, sv, i),
        }

        short_components = {
            "SQZ Top Exhaustion": recent_score_at(ind["sqz_top_exhaustion"], cfg.short_orange_arrow_score, sv, i),
            "SQZ Bearish Divergence": recent_score_at(ind["sqz_bear_div"], cfg.short_bear_div_score, sv, i),
            "RSI Bearish Divergence": recent_score_at(ind["rsi_bear_div"], cfg.short_rsi_div_score, sv, i),
            "RSI Overbought Entry": recent_score_at(ind["rsi_ob_entry"], cfg.short_rsi_ob_score, sv, i),
            "WT Bearish Divergence": recent_score_at(ind["wt_bear_div"], cfg.short_wt_div_score, sv, i),
            "WT Overbought Entry": recent_score_at(ind["wt_ob_entry"], cfg.short_wt_ob_score, sv, i),
            "Bearish Engulfing": recent_score_at(ind["bear_eng"], cfg.short_eng_score, sv, i),
            "Bearish OB": recent_score_at(smc.inside_bearish_ob, cfg.ob_entry_score, sv, i),
        }

        long_score = int(sum(long_components.values()))
        short_score = int(sum(short_components.values()))

        long_score_ok = long_score >= cfg.required_entry_score
        short_score_ok = short_score >= cfg.required_entry_score

        long_saturation_raw = bool(
            long_components["RSI Oversold Entry"] > 0
            or long_components["WT Oversold Entry"] > 0
            or ind["in_os"][i]
            or ind["wt_os_zone"][i]
            or ind["wt_os_extreme"][i]
        )
        short_saturation_raw = bool(
            short_components["RSI Overbought Entry"] > 0
            or short_components["WT Overbought Entry"] > 0
            or ind["in_ob"][i]
            or ind["wt_ob_zone"][i]
            or ind["wt_ob_extreme"][i]
        )

        long_divergence_raw = bool(
            long_components["SQZ Bullish Divergence"] > 0
            or long_components["RSI Bullish Divergence"] > 0
            or long_components["WT Bullish Divergence"] > 0
        )
        short_divergence_raw = bool(
            short_components["SQZ Bearish Divergence"] > 0
            or short_components["RSI Bearish Divergence"] > 0
            or short_components["WT Bearish Divergence"] > 0
        )

        long_zone_ok = (not cfg.use_zone_touch_filter) or bool(smc.touched_bullish_recent[i])
        short_zone_ok = (not cfg.use_zone_touch_filter) or bool(smc.touched_bearish_recent[i])

        long_saturation_ok = (not cfg.use_saturation_score_filter) or long_saturation_raw
        short_saturation_ok = (not cfg.use_saturation_score_filter) or short_saturation_raw

        long_divergence_ok = (not cfg.use_divergence_score_filter) or long_divergence_raw
        short_divergence_ok = (not cfg.use_divergence_score_filter) or short_divergence_raw

        long_btc_ok = (not cfg.use_btc_confirm_filter) or bool(btc_long_aligned[i])
        short_btc_ok = (not cfg.use_btc_confirm_filter) or bool(btc_short_aligned[i])

        raw_long = bool(long_score_ok and long_zone_ok and long_saturation_ok and long_divergence_ok and long_btc_ok)
        raw_short = bool(short_score_ok and short_zone_ok and short_saturation_ok and short_divergence_ok and short_btc_ok)

        prefer_long = bool(raw_long and ((not raw_short) or long_score >= short_score))
        prefer_short = bool(raw_short and ((not raw_long) or short_score > long_score))

        enter_long = bool(prefer_long and (i - last_long_entry_bar > cfg.score_validity_bars))
        enter_short = bool(prefer_short and (i - last_short_entry_bar > cfg.score_validity_bars))

        if enter_long:
            last_long_entry_bar = i
        if enter_short:
            last_short_entry_bar = i

        if i == n - 1:
            if enter_long:
                final_side = "BUY"
                final_components = dict(long_components)
                final_long_score = long_score
                final_short_score = short_score
                final_filters = {
                    "1 Active Valid Bullish OB Touch": bool(long_zone_ok),
                    "2 Saturation / OS": bool(long_saturation_ok),
                    "3 Scored Divergence": bool(long_divergence_ok),
                    "4 BTC Same-Direction": bool(long_btc_ok),
                }
            elif enter_short:
                final_side = "SELL"
                final_components = dict(short_components)
                final_long_score = long_score
                final_short_score = short_score
                final_filters = {
                    "1 Active Valid Bearish OB Touch": bool(short_zone_ok),
                    "2 Saturation / OB": bool(short_saturation_ok),
                    "3 Scored Divergence": bool(short_divergence_ok),
                    "4 BTC Same-Direction": bool(short_btc_ok),
                }

    if final_side is None:
        return None

    i = n - 1
    close_price = float(c[i])
    atr14 = float(ind["atr14"][i]) if is_finite(ind["atr14"][i]) else math.nan

    if final_side == "BUY":
        tp = close_price * (1.0 + cfg.tp_percent / 100.0)
        if is_finite(smc.active_sl_long[i]):
            sl = float(smc.active_sl_long[i])
        elif is_finite(atr14):
            sl = float(l[i] - atr14 * 1.5)
        else:
            sl = float(l[i])
    else:
        tp = close_price * (1.0 - cfg.tp_percent / 100.0)
        if is_finite(smc.active_sl_short[i]):
            sl = float(smc.active_sl_short[i])
        elif is_finite(atr14):
            sl = float(h[i] + atr14 * 1.5)
        else:
            sl = float(h[i])

    return SignalResult(
        symbol=symbol,
        side=final_side,
        timestamp=int(times[i]),
        timeframe=cfg.timeframe,
        close=close_price,
        tp=float(tp),
        sl=float(sl),
        long_score=int(final_long_score),
        short_score=int(final_short_score),
        components=final_components,
        filters=final_filters,
    )


# =============================================================================
# BITUNIX FUTURES REST CLIENT  (replaces CCXT)
# =============================================================================

BITUNIX_FAPI = "https://fapi.bitunix.com/api/v1/futures"
BITUNIX_KLINE_MAX = 200                # حداکثر تعداد کندل در هر درخواست
BITUNIX_NOT_TRADEABLE_CODE = 20015     # نماد دیلیست / غیرقابل معامله
BITUNIX_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}


class SymbolNotTradeable(Exception):
    pass


class BitunixFutures:
    """Async client for Bitunix USDT-M futures public market endpoints."""

    def __init__(self, session: aiohttp.ClientSession, max_retries: int = 4):
        self.session = session
        self.max_retries = max_retries
        self.not_tradeable: set = set()

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{BITUNIX_FAPI}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=25)) as r:
                    if r.status == 429:
                        await asyncio.sleep(2.0 * (attempt + 1))
                        last_err = RuntimeError("HTTP 429 rate limit")
                        continue
                    if r.status >= 500:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        last_err = RuntimeError(f"HTTP {r.status}")
                        continue
                    r.raise_for_status()
                    return await r.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                await asyncio.sleep(1.0 * (attempt + 1))
        raise RuntimeError(f"Bitunix request failed: {path} {params} -> {last_err}")

    async def fetch_usdt_symbols(self) -> List[str]:
        """List of all tradeable USDT perpetual symbols, e.g. BTCUSDT."""
        # اول trading_pairs (اطلاعات کامل‌تر)، در صورت خطا tickers
        try:
            data = await self._get("/market/trading_pairs")
            if data.get("code") == 0 and data.get("data"):
                out = []
                for m in data["data"]:
                    sym = str(m.get("symbol", "")).upper()
                    quote = str(m.get("quoteCoin", m.get("quote", ""))).upper()
                    status = str(m.get("symbolStatus", m.get("status", ""))).upper()
                    if not sym.endswith("USDT"):
                        continue
                    if quote and quote != "USDT":
                        continue
                    if status and status not in ("OPEN", "TRADING", "ONLINE", "NORMAL", ""):
                        continue
                    out.append(sym)
                if out:
                    return sorted(set(out))
        except Exception as e:
            print(f"[Bitunix] trading_pairs failed, falling back to tickers: {e}")

        data = await self._get("/market/tickers")
        if data.get("code") != 0:
            raise RuntimeError(f"Bitunix tickers failed: {data}")
        return sorted({str(x["symbol"]).upper() for x in data["data"] if str(x["symbol"]).upper().endswith("USDT")})

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> List[List[float]]:
        """
        Returns [[timestamp_ms, open, high, low, close, volume], ...] ascending,
        paginating backwards with endTime (200 per request).
        """
        if timeframe not in BITUNIX_INTERVALS:
            raise ValueError(f"Bitunix unsupported interval: {timeframe}")
        if symbol in self.not_tradeable:
            raise SymbolNotTradeable(symbol)

        tf_ms = timeframe_to_ms(timeframe)
        buckets: Dict[int, List[float]] = {}
        end = now_ms()
        target = limit + 5
        max_pages = math.ceil(target / BITUNIX_KLINE_MAX) + 3

        for _ in range(max_pages):
            if len(buckets) >= target:
                break

            data = await self._get(
                "/market/kline",
                {"symbol": symbol, "interval": timeframe, "limit": BITUNIX_KLINE_MAX, "endTime": end},
            )
            code = data.get("code")
            rows = data.get("data")

            if code not in (0, None):
                if code == BITUNIX_NOT_TRADEABLE_CODE:
                    self.not_tradeable.add(symbol)
                    raise SymbolNotTradeable(f"{symbol}: {data.get('msg')}")
                raise RuntimeError(f"{symbol} kline error code={code} msg={data.get('msg')}")

            if not rows:
                break

            oldest: Optional[int] = None
            for k in rows:
                t = int(k["time"])
                buckets[t] = [
                    t,
                    float(k["open"]),
                    float(k["high"]),
                    float(k["low"]),
                    float(k["close"]),
                    float(k.get("baseVol", k.get("volume", 0.0)) or 0.0),
                ]
                oldest = t if oldest is None else min(oldest, t)

            if oldest is None:
                break
            new_end = oldest - tf_ms
            if new_end >= end:
                break
            end = new_end

        rows_out = [buckets[t] for t in sorted(buckets)]
        return rows_out[-limit:] if limit > 0 else rows_out


async def fetch_ohlcv_df(client: BitunixFutures, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    rows = await client.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["timestamp", "open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].astype(np.int64)

    # حذف کندلِ باز فعلی: سیگنال فقط روی کندل بسته‌شده (barstate.isconfirmed)
    tf_ms = timeframe_to_ms(timeframe)
    df = df[df["timestamp"] + tf_ms <= now_ms() - 3000].reset_index(drop=True)

    if len(df) == 0:
        return None
    return df


# =============================================================================
# TELEGRAM / PERSISTENCE
# =============================================================================

class SignalStore:
    def __init__(self, path: str):
        self.path = path
        self.sent: Dict[str, int] = {}
        self.load()

    def load(self) -> None:
        if not os.path.exists(self.path):
            self.sent = {}
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.sent = {str(k): int(v) for k, v in data.items()} if isinstance(data, dict) else {}
        except Exception:
            self.sent = {}

    def save(self) -> None:
        if len(self.sent) > 20000:
            items = sorted(self.sent.items(), key=lambda kv: kv[1])[-10000:]
            self.sent = dict(items)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.sent, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    @staticmethod
    def key(sig: SignalResult) -> str:
        return f"{sig.symbol}|{sig.side}|{sig.timeframe}|{sig.timestamp}"

    def was_sent(self, sig: SignalResult) -> bool:
        return self.key(sig) in self.sent

    def mark_sent(self, sig: SignalResult) -> None:
        self.sent[self.key(sig)] = now_ms()
        self.save()


def format_signal_message(sig: SignalResult) -> str:
    dt = pd.to_datetime(sig.timestamp, unit="ms", utc=True).strftime("%Y-%m-%d %H:%M UTC")
    side_title = "🟢 BUY / LONG" if sig.side == "BUY" else "🔴 SELL / SHORT"
    sym = html.escape(sig.symbol)

    active_components = [f"• {html.escape(k)}: +{v}" for k, v in sig.components.items() if int(v) > 0]
    comp_text = "\n".join(active_components) if active_components else "—"
    filter_text = "\n".join(f"• {html.escape(k)}: {'✅' if v else '❌'}" for k, v in sig.filters.items())

    return f"""
<b>{side_title}</b>
<b>Exchange:</b> Bitunix Futures
<b>Symbol:</b> {sym}
<b>Timeframe:</b> {html.escape(sig.timeframe)}
<b>Closed Candle:</b> {html.escape(dt)}

<b>Entry Close:</b> {fmt_price(sig.close)}
<b>TP:</b> {fmt_price(sig.tp)}
<b>SL:</b> {fmt_price(sig.sl)}

<b>Long Score:</b> {sig.long_score}
<b>Short Score:</b> {sig.short_score}

<b>Active Components:</b>
{comp_text}

<b>Filters:</b>
{filter_text}
""".strip()


def _onoff(v: bool) -> str:
    return "✅ ON" if v else "❌ OFF"


def format_filters_text(cfg: Config) -> str:
    return (
        f"1️⃣ لمس ناحیه OB معتبر (آبی/قرمز): {_onoff(cfg.use_zone_touch_filter)}\n"
        f"2️⃣ اشباع خرید/فروش (RSI/WT): {_onoff(cfg.use_saturation_score_filter)}\n"
        f"3️⃣ واگرایی امتیازدار (SQZ/RSI/WT): {_onoff(cfg.use_divergence_score_filter)}\n"
        f"4️⃣ تأیید هم‌جهت بیت‌کوین: {_onoff(cfg.use_btc_confirm_filter)}"
    )


def format_startup_message(cfg: Config, symbols_count: int, last_btc_candle: str) -> str:
    return (
        "🤖 <b>ربات سیگنال Bitunix Futures روشن شد</b>\n"
        "✅ اتصال به Bitunix برقرار است\n"
        "✅ اتصال به تلگرام برقرار است\n\n"
        f"⏱ تایم‌فریم: <code>{html.escape(cfg.timeframe)}</code>\n"
        f"📊 تعداد نمادها: <code>{symbols_count}</code>\n"
        f"₿ نماد تأیید BTC: <code>{cfg.btc_symbol}</code> ({html.escape(cfg.btc_timeframe)})\n"
        f"🕯 آخرین کندل بسته BTC: <code>{html.escape(last_btc_candle)}</code>\n"
        f"🎯 حدنصاب امتیاز: <code>{cfg.required_entry_score}</code>\n"
        f"💰 TP: <code>{cfg.tp_percent}%</code>\n"
        f"🔁 فاصله اسکن: <code>{cfg.poll_seconds}s</code>\n"
        f"🧪 حالت تست (DRY_RUN): {_onoff(cfg.dry_run)}\n\n"
        "<b>فیلترهای فعال:</b>\n"
        f"{format_filters_text(cfg)}"
    )


def format_scan_start_message(cfg: Config, symbols_count: int, scan_no: int) -> str:
    now_txt = pd.Timestamp.now("UTC").strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"🔍 <b>شروع اسکن #{scan_no}</b>\n"
        f"🕒 {now_txt}\n"
        f"⏱ TF: <code>{html.escape(cfg.timeframe)}</code> | نمادها: <code>{symbols_count}</code>\n\n"
        "<b>فیلترهای روشن:</b>\n"
        f"{format_filters_text(cfg)}"
    )


def format_scan_done_message(cfg: Config, scan_no: int, n_signals: int, duration_s: float,
                             btc_long: Optional[bool], btc_short: Optional[bool]) -> str:
    if btc_long is None:
        btc_txt = "OFF / نامشخص"
    else:
        btc_txt = f"LONG={'✅' if btc_long else '❌'}  SHORT={'✅' if btc_short else '❌'}"
    return (
        f"✅ <b>اسکن #{scan_no} تمام شد</b>\n"
        f"📨 سیگنال‌های پیدا شده: <code>{n_signals}</code>\n"
        f"₿ وضعیت BTC: {btc_txt}\n"
        f"⏳ مدت: <code>{duration_s:.0f}s</code>"
    )


async def send_telegram(session: aiohttp.ClientSession, cfg: Config, text: str) -> bool:
    if cfg.dry_run or not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        print("\n========== TELEGRAM (DRY-RUN / NOT CONFIGURED) ==========")
        print(text)
        print("=========================================================\n")
        return True

    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": cfg.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    for attempt in range(3):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                body = await resp.text()
                if resp.status == 200:
                    return True
                print(f"[Telegram Error] status={resp.status} body={body}")
        except Exception as e:
            print(f"[Telegram Exception] attempt={attempt + 1}: {e}")
        await asyncio.sleep(2)
    return False


# =============================================================================
# SCANNER LOOP
# =============================================================================

async def process_symbol(
    client: BitunixFutures,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    cfg: Config,
    btc_context: Optional[Tuple[np.ndarray, np.ndarray, np.ndarray]],
    store: SignalStore,
) -> Optional[SignalResult]:
    try:
        async with sem:
            df = await fetch_ohlcv_df(client, symbol, cfg.timeframe, cfg.ohlcv_limit)

        if df is None or len(df) < cfg.min_bars:
            return None

        # محاسبات سنگین CPU را داخل thread می‌بریم تا event loop قفل نشود
        sig = await asyncio.to_thread(analyze_symbol, symbol, df, cfg, btc_context)
        if sig is None:
            return None

        if store.was_sent(sig):
            return sig

        text = format_signal_message(sig)
        ok = await send_telegram(session, cfg, text)
        if ok:
            store.mark_sent(sig)
            print(f"[SIGNAL SENT] {sig.side} {symbol} @ {fmt_price(sig.close)} candle={sig.timestamp}")
        else:
            print(f"[SIGNAL NOT SENT] Telegram failed: {sig.side} {symbol}")
        return sig

    except SymbolNotTradeable:
        return None
    except Exception as e:
        print(f"[ERROR] {symbol}: {e}")
        return None


async def build_btc_context(client: BitunixFutures, cfg: Config) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray]]:
    if not cfg.use_btc_confirm_filter:
        return None
    try:
        btc_df = await fetch_ohlcv_df(client, cfg.btc_symbol, cfg.btc_timeframe, cfg.ohlcv_limit)
        if btc_df is None or len(btc_df) < cfg.min_bars:
            print("[BTC] Not enough BTC candles; BTC filter will block signals until data is available.")
            return None

        btc_ind = compute_indicators(btc_df, cfg)
        btc_long, btc_short = compute_btc_points(btc_ind, cfg)
        btc_times = btc_df["timestamp"].to_numpy(dtype=np.int64)

        last_dt = pd.to_datetime(int(btc_times[-1]), unit="ms", utc=True).strftime("%Y-%m-%d %H:%M UTC")
        print(f"[BTC] {cfg.btc_symbol} last candle={last_dt} long_ok={bool(btc_long[-1])} short_ok={bool(btc_short[-1])}")
        return btc_times, btc_long, btc_short
    except Exception as e:
        print(f"[BTC ERROR] {e}")
        print(traceback.format_exc())
        return None


async def scan_once(
    client: BitunixFutures,
    session: aiohttp.ClientSession,
    symbols: List[str],
    cfg: Config,
    store: SignalStore,
    scan_no: int = 1,
) -> None:
    print(f"\n[SCAN #{scan_no}] {pd.Timestamp.now('UTC').strftime('%Y-%m-%d %H:%M:%S UTC')} | symbols={len(symbols)}")
    t0 = time.time()

    # پیام شروع اسکن + فیلترهای روشن
    if cfg.send_scan_start_message:
        await send_telegram(session, cfg, format_scan_start_message(cfg, len(symbols), scan_no))

    btc_context = await build_btc_context(client, cfg)
    btc_long = bool(btc_context[1][-1]) if btc_context is not None else None
    btc_short = bool(btc_context[2][-1]) if btc_context is not None else None

    sem = asyncio.Semaphore(cfg.max_concurrent_requests)
    tasks = [
        process_symbol(client, session, sem, symbol, cfg, btc_context, store)
        for symbol in symbols
        if symbol not in client.not_tradeable
    ]
    results = await asyncio.gather(*tasks)
    signals = [r for r in results if r is not None]

    duration = time.time() - t0
    print(f"[SCAN DONE] detected_signals={len(signals)} duration={duration:.0f}s "
          f"delisted={len(client.not_tradeable)}")

    if cfg.send_scan_start_message:
        await send_telegram(session, cfg, format_scan_done_message(cfg, scan_no, len(signals), duration, btc_long, btc_short))


def seconds_until_next_close(timeframe: str, buffer_s: int = 8) -> float:
    tf_ms = timeframe_to_ms(timeframe)
    now = now_ms()
    next_close = (now // tf_ms + 1) * tf_ms
    return max(1.0, (next_close - now) / 1000.0 + buffer_s)


async def main() -> None:
    if CFG.timeframe not in BITUNIX_INTERVALS:
        raise SystemExit(f"TIMEFRAME نامعتبر برای Bitunix: {CFG.timeframe}")

    if not CFG.dry_run and (not CFG.telegram_bot_token or not CFG.telegram_chat_id):
        print("⚠️  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID ست نشده؛ سیگنال‌ها فقط در کنسول چاپ می‌شوند.")

    headers = {"User-Agent": "bitunix-smc-scanner/1.0", "Accept": "application/json"}
    connector = aiohttp.TCPConnector(limit=CFG.max_concurrent_requests * 2)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        client = BitunixFutures(session)

        # ---- connectivity check ----
        try:
            symbols = await client.fetch_usdt_symbols()
        except Exception as e:
            raise SystemExit(f"❌ اتصال به Bitunix ناموفق بود: {e}")

        if CFG.symbols_whitelist:
            white = set(CFG.symbols_whitelist)
            symbols = [s for s in symbols if s in white]
        if CFG.symbols_blacklist:
            black = set(s.upper() for s in CFG.symbols_blacklist)
            symbols = [s for s in symbols if s not in black]
        if CFG.max_symbols > 0:
            symbols = symbols[:CFG.max_symbols]

        if not symbols:
            raise SystemExit("هیچ مارکت فیوچرز USDT در Bitunix پیدا نشد.")

        # quick kline sanity check on BTC
        test_df = await fetch_ohlcv_df(client, CFG.btc_symbol, CFG.timeframe, 50)
        if test_df is None or test_df.empty:
            raise SystemExit("❌ دریافت کندل BTCUSDT از Bitunix ناموفق بود.")
        last_dt = pd.to_datetime(int(test_df['timestamp'].iloc[-1]), unit='ms', utc=True)

        print("==============================================")
        print(" Bitunix SMC+SQZ+RSI+WT+ENG Scanner Started")
        print("==============================================")
        print(f"✅ Bitunix connected | last closed BTC {CFG.timeframe} candle: {last_dt:%Y-%m-%d %H:%M UTC}")
        print(f"Timeframe: {CFG.timeframe}")
        print(f"Symbols: {len(symbols)}")
        print(f"BTC Confirmation Symbol: {CFG.btc_symbol} ({CFG.btc_timeframe})")
        print(f"Telegram configured: {bool(CFG.telegram_bot_token and CFG.telegram_chat_id)}  dry_run={CFG.dry_run}")
        print("4 Filters:")
        print(f"  1 OB Touch Valid: {CFG.use_zone_touch_filter}")
        print(f"  2 Saturation: {CFG.use_saturation_score_filter}")
        print(f"  3 Divergence: {CFG.use_divergence_score_filter}")
        print(f"  4 BTC Confirm: {CFG.use_btc_confirm_filter}")
        print("==============================================")

        store = SignalStore(CFG.sent_store_file)

        # ---- پیام اتصال به ربات تلگرام ----
        if CFG.send_startup_message:
            ok = await send_telegram(
                session, CFG,
                format_startup_message(CFG, len(symbols), f"{last_dt:%Y-%m-%d %H:%M UTC}"),
            )
            if ok and not CFG.dry_run and CFG.telegram_bot_token:
                print("✅ Telegram connected: startup message sent.")
            elif not ok:
                print("❌ Telegram startup message FAILED — توکن / chat_id را بررسی کنید.")

        scan_no = 0
        while True:
            scan_no += 1
            start = time.time()
            try:
                await scan_once(client, session, symbols, CFG, store, scan_no)
            except Exception as e:
                print(f"[SCAN LOOP ERROR] {e}")
                print(traceback.format_exc())

            elapsed = time.time() - start
            # اسکن بعدی: هر poll_seconds، ولی حداکثر تا بسته‌شدن کندل بعدی صبر می‌کنیم
            sleep_for = max(5.0, CFG.poll_seconds - elapsed)
            sleep_for = min(sleep_for, seconds_until_next_close(CFG.timeframe))
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
