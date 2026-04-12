"""Shared technical-indicator helpers for lightweight strategy logic."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def calculate_wilder_rsi(values: Iterable[float], period: int = 14) -> float | None:
    """Return the latest Wilder RSI value for a close series."""
    if period <= 0:
        raise ValueError("RSI period must be greater than zero.")

    series = pd.Series(list(values), dtype="float64").dropna().reset_index(drop=True)
    if len(series) < period + 1:
        return None

    deltas = series.diff().dropna().reset_index(drop=True)
    gains = deltas.clip(lower=0)
    losses = -deltas.clip(upper=0)

    average_gain = float(gains.iloc[:period].mean())
    average_loss = float(losses.iloc[:period].mean())

    for index in range(period, len(deltas)):
        average_gain = ((average_gain * (period - 1)) + float(gains.iloc[index])) / period
        average_loss = ((average_loss * (period - 1)) + float(losses.iloc[index])) / period

    if average_loss == 0:
        if average_gain == 0:
            return 50.0
        return 100.0

    relative_strength = average_gain / average_loss
    return float(100 - (100 / (1 + relative_strength)))
