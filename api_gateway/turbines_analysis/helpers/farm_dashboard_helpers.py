"""
Helpers for farm dashboard monthly analysis.

Focus:
- Parse selected indicator keys from request params
- Month bucket utilities (month_start_ms)
- Aggregation rules for indicators (sum vs avg)
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from analytics.models import IndicatorData
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms


# Indicators aggregation semantics at farm level:
# - sum: sum across turbines for a month
# - avg: average across turbines for a month (ignore None)
INDICATOR_AGG_SUM = {
    "RealEnergy",
    "ReachableEnergy",
    "LossEnergy",
    "StopLoss",
    "PartialStopLoss",
    "UnderProductionLoss",
    "CurtailmentLoss",
    "PartialCurtailmentLoss",
    "TotalStopPoints",
    "TotalPartialStopPoints",
    "TotalUnderProductionPoints",
    "TotalCurtailmentPoints",
    "FailureCount",
}

INDICATOR_AGG_AVG = {
    "AverageWindSpeed",
    "LossPercent",
    "Tba",
    "Pba",
    "Mtbf",
    "Mttr",
    "Mttf",
    "CapacityFactor",
    "YawMisalignment",
}

# Mapping from API indicator keys to IndicatorData model fields.
# Note: some dashboard indicators (e.g. DailyProduction) are derived from other tables.
INDICATOR_KEY_TO_FIELD: Dict[str, str] = {
    "AverageWindSpeed": "average_wind_speed",
    "ReachableEnergy": "reachable_energy",
    "RealEnergy": "real_energy",
    "LossEnergy": "loss_energy",
    "LossPercent": "loss_percent",
    "StopLoss": "stop_loss",
    "PartialStopLoss": "partial_stop_loss",
    "UnderProductionLoss": "under_production_loss",
    "CurtailmentLoss": "curtailment_loss",
    "PartialCurtailmentLoss": "partial_curtailment_loss",
    "TotalStopPoints": "total_stop_points",
    "TotalPartialStopPoints": "total_partial_stop_points",
    "TotalUnderProductionPoints": "total_under_production_points",
    "TotalCurtailmentPoints": "total_curtailment_points",
    "RatedPower": "rated_power",
    "CapacityFactor": "capacity_factor",
    "Tba": "tba",
    "Pba": "pba",
    "FailureCount": "failure_count",
    "Mtbf": "mtbf",
    "Mttr": "mttr",
    "Mttf": "mttf",
    "TimeStep": "time_step",
    "TotalDuration": "total_duration",
    "DurationWithoutError": "duration_without_error",
    "YawMisalignment": "yaw_misalignment",
    "UpPeriodsCount": "up_periods_count",
    "DownPeriodsCount": "down_periods_count",
    "UpPeriodsDuration": "up_periods_duration",
    "DownPeriodsDuration": "down_periods_duration",
    "AepWeibullTurbine": "aep_weibull_turbine",
    "AepWeibullWindFarm": "aep_weibull_wind_farm",
    "AepRayleighMeasured4": "aep_rayleigh_measured_4",
    "AepRayleighMeasured5": "aep_rayleigh_measured_5",
    "AepRayleighMeasured6": "aep_rayleigh_measured_6",
    "AepRayleighMeasured7": "aep_rayleigh_measured_7",
    "AepRayleighMeasured8": "aep_rayleigh_measured_8",
    "AepRayleighMeasured9": "aep_rayleigh_measured_9",
    "AepRayleighMeasured10": "aep_rayleigh_measured_10",
    "AepRayleighMeasured11": "aep_rayleigh_measured_11",
    "AepRayleighExtrapolated4": "aep_rayleigh_extrapolated_4",
    "AepRayleighExtrapolated5": "aep_rayleigh_extrapolated_5",
    "AepRayleighExtrapolated6": "aep_rayleigh_extrapolated_6",
    "AepRayleighExtrapolated7": "aep_rayleigh_extrapolated_7",
    "AepRayleighExtrapolated8": "aep_rayleigh_extrapolated_8",
    "AepRayleighExtrapolated9": "aep_rayleigh_extrapolated_9",
    "AepRayleighExtrapolated10": "aep_rayleigh_extrapolated_10",
    "AepRayleighExtrapolated11": "aep_rayleigh_extrapolated_11",
}


def get_indicator_value(ind: IndicatorData, key: str) -> Optional[float]:
    """Read numeric indicator value from IndicatorData by API key."""
    field = INDICATOR_KEY_TO_FIELD.get(key)
    if not field:
        return None
    val = getattr(ind, field, None)
    return None if val is None else float(val)


def parse_indicator_keys(raw: Sequence[str]) -> List[str]:
    """
    Parse indicators from query params.

    Supports:
    - repeated param: indicators=RealEnergy&indicators=LossPercent
    - comma-separated: indicators=RealEnergy,LossPercent
    """
    out: List[str] = []
    for item in raw:
        if not item:
            continue
        parts = [p.strip() for p in str(item).split(",")]
        for p in parts:
            if p and p not in out:
                out.append(p)
    return out


def month_start_ms_from_datetime(dt: datetime | date) -> int:
    """Return UTC month-start timestamp in ms for given date/datetime.

    Note: `TruncMonth(DateField)` can yield `datetime.date` (no tzinfo).
    """
    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    ms = datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)
    return int(ms.timestamp() * 1000)


def month_start_ms_from_date_parts(year: int, month: int) -> int:
    """Return UTC month-start timestamp in ms from year/month."""
    ms = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
    return int(ms.timestamp() * 1000)


def month_start_ms_from_ms(ts_ms: int) -> int:
    """Return UTC month-start timestamp in ms for a millisecond epoch timestamp.
    
    Args:
        ts_ms: Timestamp (will be normalized to milliseconds if needed)
    """
    # Normalize to milliseconds (handle legacy data in seconds)
    ts_ms = to_epoch_ms(ts_ms) or ts_ms
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return month_start_ms_from_datetime(dt)


def get_months_in_range(start_ms: int, end_ms: int) -> List[int]:
    """Return list of month_start_ms timestamps for all months covered by the time range.
    
    Args:
        start_ms: Start timestamp in milliseconds
        end_ms: End timestamp in milliseconds
    
    Returns:
        List of month_start_ms values (in milliseconds) for all months that the range covers.
        Includes the month containing start_ms and the month containing end_ms.
    """
    # Normalize timestamps to milliseconds
    start_ms = to_epoch_ms(start_ms) or start_ms
    end_ms = to_epoch_ms(end_ms) or end_ms
    
    if start_ms > end_ms:
        return []
    
    # Get the month start for the start timestamp
    start_dt = datetime.fromtimestamp(int(start_ms) / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(int(end_ms) / 1000.0, tz=timezone.utc)
    
    months: List[int] = []
    current = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
    end_month = datetime(end_dt.year, end_dt.month, 1, tzinfo=timezone.utc)
    
    while current <= end_month:
        months.append(int(current.timestamp() * 1000))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months


def aggregate_values(values: Iterable[Optional[float]], mode: str) -> Optional[float]:
    """Aggregate numeric values using mode in ('sum','avg'). Ignore None."""
    vv = [v for v in values if v is not None]
    if not vv:
        return None
    if mode == "sum":
        return float(sum(vv))
    # default avg
    return float(sum(vv) / float(len(vv)))


def indicator_agg_mode(indicator_key: str) -> str:
    """Return aggregation mode for indicator key: 'sum' or 'avg'."""
    if indicator_key in INDICATOR_AGG_SUM:
        return "sum"
    if indicator_key in INDICATOR_AGG_AVG:
        return "avg"
    # Fallback: avg is safer for ratios/means
    return "avg"

