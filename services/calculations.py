"""Calculation helpers for market data lookups."""

from __future__ import annotations

from typing import Optional

import pandas as pd


def calculate_point_change(current_close: float, prior_close: float) -> Optional[float]:
    """Return the point change between two closes."""
    if prior_close in (None, 0) or pd.isna(prior_close) or pd.isna(current_close):
        return None
    return round(float(current_close) - float(prior_close), 2)


def calculate_percent_change(current_close: float, prior_close: float) -> Optional[float]:
    """Return the percent change between two closes."""
    if prior_close in (None, 0) or pd.isna(prior_close) or pd.isna(current_close):
        return None

    point_change = float(current_close) - float(prior_close)
    return round((point_change / float(prior_close)) * 100, 2)


def add_daily_change_columns(dataframe: pd.DataFrame, close_column: str = "Close") -> pd.DataFrame:
    """Add daily point and percent change columns based on the close column."""
    frame = dataframe.copy()
    prior_close = frame[close_column].shift(1)

    frame["Daily Point Change"] = (frame[close_column] - prior_close).round(2)
    frame["Daily Percent Change"] = ((frame[close_column] - prior_close) / prior_close * 100).round(2)

    return frame
