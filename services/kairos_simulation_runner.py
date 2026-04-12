"""Deterministic Kairos Phase 3B simulation tapes and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
import math
from typing import Dict, Optional, Sequence, Tuple


SESSION_BAR_COUNT = 390
SESSION_START = time(8, 30)


@dataclass(frozen=True)
class KairosTapeBar:
    """Single one-minute ES-style bar used by the Kairos simulator."""

    bar_index: int
    minute_offset: int
    simulated_time: time
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class KairosTapeCheckpoint:
    """Automatic pause checkpoint for future user-input decisions."""

    bar_index: int
    reason: str
    title: str
    detail: str


@dataclass(frozen=True)
class KairosTapeScenario:
    """Full 390-bar scenario for the Kairos tape simulator."""

    key: str
    name: str
    description: str
    bars: Tuple[KairosTapeBar, ...]
    vix_series: Tuple[float, ...]
    checkpoints: Tuple[KairosTapeCheckpoint, ...] = ()

    def checkpoint_at(self, bar_index: int) -> Optional[KairosTapeCheckpoint]:
        for checkpoint in self.checkpoints:
            if checkpoint.bar_index == bar_index:
                return checkpoint
        return None


def build_scenario(
    *,
    key: str,
    name: str,
    description: str,
    price_keyframes: Sequence[tuple[int, float]],
    vix_keyframes: Sequence[tuple[int, float]],
    checkpoints: Sequence[KairosTapeCheckpoint] = (),
    price_noise: float = 0.42,
    vix_noise: float = 0.14,
    volume_base: int = 900,
) -> KairosTapeScenario:
    """Build and validate a full-day Kairos scenario tape."""

    bars = tuple(_build_tape_bars(price_keyframes, price_noise=price_noise, volume_base=volume_base))
    vix_series = tuple(_build_scalar_series(vix_keyframes, amplitude=vix_noise, period=21.0, clamp_min=12.0))
    scenario = KairosTapeScenario(
        key=key,
        name=name,
        description=description,
        bars=bars,
        vix_series=vix_series,
        checkpoints=tuple(sorted(checkpoints, key=lambda item: item.bar_index)),
    )
    validate_tape_scenario(scenario)
    return scenario


def validate_tape_scenario(scenario: KairosTapeScenario) -> None:
    """Validate a scenario so future imported tapes can reuse the same format."""

    if len(scenario.bars) != SESSION_BAR_COUNT:
        raise ValueError(f"{scenario.name} must contain exactly {SESSION_BAR_COUNT} bars.")
    if len(scenario.vix_series) != SESSION_BAR_COUNT:
        raise ValueError(f"{scenario.name} must contain exactly {SESSION_BAR_COUNT} VIX points.")

    for expected_index, bar in enumerate(scenario.bars):
        if bar.bar_index != expected_index:
            raise ValueError(f"{scenario.name} bar index mismatch at position {expected_index}.")
        if bar.minute_offset != expected_index:
            raise ValueError(f"{scenario.name} minute offset mismatch at bar {expected_index}.")
        if bar.simulated_time != tape_time_for_index(expected_index):
            raise ValueError(f"{scenario.name} simulated time mismatch at bar {expected_index}.")
        if bar.high < max(bar.open, bar.close):
            raise ValueError(f"{scenario.name} high is invalid at bar {expected_index}.")
        if bar.low > min(bar.open, bar.close):
            raise ValueError(f"{scenario.name} low is invalid at bar {expected_index}.")
        if bar.volume <= 0:
            raise ValueError(f"{scenario.name} volume must be positive at bar {expected_index}.")

    for checkpoint in scenario.checkpoints:
        if checkpoint.bar_index < 0 or checkpoint.bar_index >= SESSION_BAR_COUNT:
            raise ValueError(f"{scenario.name} checkpoint is outside the valid tape range.")


def tape_time_for_index(bar_index: int) -> time:
    """Return the simulated one-minute session time for a bar index."""

    anchor = datetime(2000, 1, 1, SESSION_START.hour, SESSION_START.minute) + timedelta(minutes=bar_index)
    return anchor.time()


def _build_scalar_series(
    keyframes: Sequence[tuple[int, float]],
    *,
    amplitude: float,
    period: float,
    clamp_min: float,
) -> list[float]:
    values: list[float] = []
    for bar_index in range(SESSION_BAR_COUNT):
        base = _interpolate_keyframes(bar_index, keyframes)
        wave = _wave(bar_index, amplitude, 0.45, period) + _wave(bar_index, amplitude * 0.35, 1.35, period / 2.4)
        values.append(round(max(clamp_min, base + wave), 2))
    return values


def _build_tape_bars(
    price_keyframes: Sequence[tuple[int, float]],
    *,
    price_noise: float,
    volume_base: int,
) -> list[KairosTapeBar]:
    closes = _build_scalar_series(price_keyframes, amplitude=price_noise, period=17.0, clamp_min=1.0)
    bars: list[KairosTapeBar] = []
    previous_close = round(closes[0] - 0.72, 2)

    for bar_index, close in enumerate(closes):
        open_price = previous_close if bar_index > 0 else round(closes[0] - 0.88, 2)
        body = abs(close - open_price)
        wick_up = 0.18 + abs(_wave(bar_index, price_noise * 0.42, 0.2, 11.0))
        wick_down = 0.18 + abs(_wave(bar_index, price_noise * 0.38, 1.1, 13.0))
        high = round(max(open_price, close) + wick_up + body * 0.18, 2)
        low = round(min(open_price, close) - wick_down - body * 0.18, 2)

        edge_profile = abs((bar_index - ((SESSION_BAR_COUNT - 1) / 2)) / ((SESSION_BAR_COUNT - 1) / 2))
        volume_profile = 0.8 + (0.9 * edge_profile)
        if bar_index < 15 or bar_index > 374:
            volume_profile += 0.3
        volume = int(max(100, volume_base * volume_profile * (1.0 + min(body, 3.0) / 6.0) + 30 * (1 + math.sin(bar_index / 5.0))))

        bars.append(
            KairosTapeBar(
                bar_index=bar_index,
                minute_offset=bar_index,
                simulated_time=tape_time_for_index(bar_index),
                open=round(open_price, 2),
                high=high,
                low=low,
                close=round(close, 2),
                volume=volume,
            )
        )
        previous_close = round(close, 2)

    return bars


def _interpolate_keyframes(position: int, keyframes: Sequence[tuple[int, float]]) -> float:
    ordered = sorted(keyframes, key=lambda item: item[0])
    if position <= ordered[0][0]:
        return ordered[0][1]
    if position >= ordered[-1][0]:
        return ordered[-1][1]

    for left, right in zip(ordered, ordered[1:]):
        left_index, left_value = left
        right_index, right_value = right
        if left_index <= position <= right_index:
            distance = right_index - left_index
            if distance == 0:
                return right_value
            progress = (position - left_index) / distance
            return left_value + ((right_value - left_value) * progress)

    return ordered[-1][1]


def _wave(position: int, amplitude: float, phase: float, period: float) -> float:
    return amplitude * math.sin((position / period) + phase)


KAIROS_RUNNER_SCENARIOS: Dict[str, KairosTapeScenario] = {
    "bearish-downtrend-day": build_scenario(
        key="bearish-downtrend-day",
        name="Bearish Downtrend Day",
        description="Opens soft, never confirms, and bleeds lower into a failed session structure.",
        price_keyframes=((0, 6116.0), (40, 6111.0), (95, 6104.0), (150, 6095.0), (235, 6089.0), (389, 6085.0)),
        vix_keyframes=((0, 18.8), (90, 19.5), (170, 20.2), (260, 20.8), (389, 21.2)),
        volume_base=980,
    ),
    "bullish-recovery-day": build_scenario(
        key="bullish-recovery-day",
        name="Bullish Recovery Day",
        description="Starts mixed, recovers into a sustained mid-morning trend, then cools late while staying constructive.",
        price_keyframes=((0, 6118.0), (25, 6115.0), (70, 6124.0), (115, 6132.0), (175, 6138.0), (275, 6132.0), (389, 6127.0)),
        vix_keyframes=((0, 18.9), (60, 19.2), (130, 19.9), (210, 20.1), (320, 19.2), (389, 18.7)),
        volume_base=940,
    ),
    "chop-no-trade-day": build_scenario(
        key="chop-no-trade-day",
        name="Chop / No Trade Day",
        description="Alternates between small pushes and reversals but never develops a trustworthy intraday edge.",
        price_keyframes=((0, 6121.0), (55, 6124.0), (110, 6120.0), (170, 6126.0), (245, 6121.0), (315, 6125.0), (389, 6120.0)),
        vix_keyframes=((0, 18.4), (90, 17.9), (180, 18.2), (280, 18.0), (389, 18.1)),
        price_noise=0.28,
        vix_noise=0.08,
        volume_base=760,
    ),
    "window-open-then-failure-day": build_scenario(
        key="window-open-then-failure-day",
        name="Window Open Then Failure Day",
        description="Builds a clean early window, then rolls over hard enough to invalidate the session before noon.",
        price_keyframes=((0, 6124.0), (42, 6130.0), (66, 6136.2), (102, 6133.3), (138, 6110.0), (220, 6072.0), (389, 6058.0)),
        vix_keyframes=((0, 19.0), (66, 20.1), (138, 20.4), (240, 19.8), (389, 19.2)),
        checkpoints=(
            KairosTapeCheckpoint(
                bar_index=66,
                reason="awaiting_user_continue",
                title="Checkpoint reached",
                detail="A future decision checkpoint was reached at the window-open area. Resume to continue the simulated tape.",
            ),
        ),
        volume_base=1020,
    ),
}


for scenario in KAIROS_RUNNER_SCENARIOS.values():
    validate_tape_scenario(scenario)