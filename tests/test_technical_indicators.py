from __future__ import annotations

import unittest

from services.technical_indicators import calculate_wilder_rsi


def _manual_wilder_rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period
    for index in range(period, len(deltas)):
        average_gain = ((average_gain * (period - 1)) + gains[index]) / period
        average_loss = ((average_loss * (period - 1)) + losses[index]) / period
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    relative_strength = average_gain / average_loss
    return 100 - (100 / (1 + relative_strength))


class TechnicalIndicatorTests(unittest.TestCase):
    def test_wilder_rsi_matches_manual_reference(self) -> None:
        closes = [
            44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08,
            45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41, 46.22, 45.64,
            46.21, 46.25, 45.71, 46.45, 45.78, 45.35, 44.03, 44.18, 44.22, 44.57,
        ]

        expected = _manual_wilder_rsi(closes, period=14)
        actual = calculate_wilder_rsi(closes, period=14)

        self.assertIsNotNone(actual)
        self.assertAlmostEqual(actual, expected, places=8)

    def test_wilder_rsi_detects_oversold_condition(self) -> None:
        closes = [120.0 - index * 2.5 for index in range(30)]

        actual = calculate_wilder_rsi(closes, period=14)

        self.assertIsNotNone(actual)
        self.assertLess(actual, 30.0)


if __name__ == "__main__":
    unittest.main()