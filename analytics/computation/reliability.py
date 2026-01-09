from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Optional

import pandas as pd


ReliabilityState = Literal["UP", "DOWN", "OTHER"]


@dataclass(frozen=True)
class FailureEvent:
    """
    A single failure/downtime interval, derived from SCADA status time-series.
    """

    start: pd.Timestamp
    end: pd.Timestamp
    duration_s: float


def _infer_resolution_seconds(index: pd.DatetimeIndex) -> float:
    if len(index) < 2:
        return 0.0
    diffs = index.to_series().diff().dropna()
    if diffs.empty:
        return 0.0
    # Use mode to be robust to occasional gaps
    dt = diffs.mode().iloc[0]
    return float(dt.total_seconds())


def _map_statuses(
    status_series: pd.Series,
    up_statuses: set[str],
    down_statuses: set[str],
    ignore_statuses: set[str],
) -> pd.Series:
    """
    Map raw status labels to ReliabilityState {UP, DOWN, OTHER}.
    - ignore_statuses => OTHER
    - up_statuses => UP
    - down_statuses => DOWN
    - any unknown label => OTHER
    """
    # If categorical, convert to string labels
    s = status_series.astype(str)

    def mapper(x: str) -> ReliabilityState:
        if x in ignore_statuses:
            return "OTHER"
        if x in up_statuses:
            return "UP"
        if x in down_statuses:
            return "DOWN"
        return "OTHER"

    return s.map(mapper)


def compute_failure_events(
    classified: pd.DataFrame,
    *,
    up_statuses: Iterable[str],
    down_statuses: Iterable[str],
    ignore_statuses: Iterable[str],
    min_down_duration_s: Optional[float] = None,
) -> tuple[list[FailureEvent], float]:
    """
    Compute failure events from time-series status (IEC 61400-26-4 inspired).

    Strict definition used (per your choice):
    - Failure event = transition UP -> DOWN (STOP) and consecutive DOWN samples form one interval.
    - Ignore statuses (MEASUREMENT_ERROR/UNKNOWN/...) do not start or end events.

    Returns: (events, resolution_seconds)
    """
    if "status" not in classified.columns:
        raise ValueError("classified DataFrame must contain a 'status' column.")
    if not isinstance(classified.index, pd.DatetimeIndex):
        raise ValueError("classified DataFrame index must be a DatetimeIndex (TIMESTAMP).")

    df = classified.sort_index()
    idx = df.index
    dt_s = _infer_resolution_seconds(idx)
    if dt_s <= 0:
        return [], 0.0

    up_set = set(up_statuses)
    down_set = set(down_statuses)
    ignore_set = set(ignore_statuses)

    state = _map_statuses(df["status"], up_set, down_set, ignore_set)

    events: list[FailureEvent] = []
    in_down = False
    down_start: Optional[pd.Timestamp] = None
    down_count = 0

    # Track last "meaningful" state (UP/DOWN) skipping OTHER for transition logic
    last_meaningful: Optional[ReliabilityState] = None

    for ts, st in state.items():
        if st == "OTHER":
            # Ignore does not change last_meaningful and does not close/open intervals
            continue

        if st == "DOWN":
            # Start a new event only if we were previously UP
            if not in_down and last_meaningful == "UP":
                in_down = True
                down_start = ts
                down_count = 1
            elif in_down:
                down_count += 1
            # If last meaningful was DOWN already but we weren't in_down (shouldn't happen),
            # treat it as continuing DOWN without counting a failure.
            last_meaningful = "DOWN"
            continue

        # st == "UP"
        if in_down:
            # Close the DOWN interval at the last DOWN timestamp
            down_end = ts - pd.Timedelta(seconds=dt_s)
            duration_s = down_count * dt_s

            if min_down_duration_s is None or duration_s >= min_down_duration_s:
                events.append(FailureEvent(start=down_start, end=down_end, duration_s=duration_s))  # type: ignore[arg-type]

            in_down = False
            down_start = None
            down_count = 0

        last_meaningful = "UP"

    # If we end while still DOWN, close at the dataset end
    if in_down and down_start is not None:
        down_end = idx.max()
        duration_s = down_count * dt_s
        if min_down_duration_s is None or duration_s >= min_down_duration_s:
            events.append(FailureEvent(start=down_start, end=down_end, duration_s=duration_s))

    return events, dt_s


def compute_mttr_mttf_mtbf(
    classified: pd.DataFrame,
    *,
    up_statuses: Iterable[str],
    down_statuses: Iterable[str],
    ignore_statuses: Iterable[str],
    min_down_duration_s: Optional[float] = None,
) -> dict:
    """
    Compute reliability metrics (strict mode) from classified status.

    Formulas (applied to SCADA time-series, IEC-style):
      FailureCount = #(UP -> STOP transitions, with consecutive STOP merged)

      MTTR = (TotalDownTime) / FailureCount
        where TotalDownTime = sum(duration of STOP intervals)

      MTTF = (TotalUpTime) / FailureCount
        where TotalUpTime counts only UP statuses (NORMAL + OVERPRODUCTION) and excludes ignore/degraded.

      MTBF = MTTF + MTTR
    """
    events, dt_s = compute_failure_events(
        classified,
        up_statuses=up_statuses,
        down_statuses=down_statuses,
        ignore_statuses=ignore_statuses,
        min_down_duration_s=min_down_duration_s,
    )
    if dt_s <= 0:
        return {
            "FailureCount": 0,
            "Mttr": None,
            "Mttf": None,
            "Mtbf": None,
        }

    up_set = set(up_statuses)
    down_set = set(down_statuses)
    ignore_set = set(ignore_statuses)
    state = _map_statuses(classified.sort_index()["status"], up_set, down_set, ignore_set)

    failure_count = len(events)
    if failure_count == 0:
        return {
            "FailureCount": 0,
            "Mttr": None,
            "Mttf": None,
            "Mtbf": None,
            "TotalDownTime": 0.0,
            "TotalUpTime": float((state == "UP").sum() * dt_s),
        }

    total_down_s = float(sum(e.duration_s for e in events))
    total_up_s = float((state == "UP").sum() * dt_s)

    mttr = total_down_s / failure_count
    mttf = total_up_s / failure_count
    mtbf = mttr + mttf

    return {
        "FailureCount": failure_count,
        "TotalDownTime": total_down_s,
        "TotalUpTime": total_up_s,
        "Mttr": mttr,
        "Mttf": mttf,
        "Mtbf": mtbf,
    }


