from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from acquisition.models import ScadaUnitConfig
from facilities.models import Turbines


CANONICAL_UNITS: Dict[str, str] = {
    "TIMESTAMP": "datetime",
    "WIND_SPEED": "m/s",
    "ACTIVE_POWER": "kW",
    "TEMPERATURE": "K",
    "PRESSURE": "Pa",
    "HUMIDITY": "ratio",
    "DIRECTION_WIND": "deg",
    "DIRECTION_NACELLE": "deg",
}


@dataclass(frozen=True)
class UnitConfigResolved:
    data_source: str
    active_power_unit: str
    wind_speed_unit: str
    temperature_unit: str
    pressure_unit: str
    humidity_unit: str
    active_power_multiplier: float = 1.0
    wind_speed_multiplier: float = 1.0
    temperature_multiplier: float = 1.0
    pressure_multiplier: float = 1.0
    humidity_multiplier: float = 1.0
    # For traceability/debug
    config_id: Optional[int] = None
    config_scope: str = "default"


def _pick_unit_config(turbine: Turbines, data_source: str) -> Optional[ScadaUnitConfig]:
    """
    Pick the best ScadaUnitConfig for this turbine and data_source.

    Priority:
      1) turbine + data_source
      2) turbine + any
      3) farm + data_source
      4) farm + any
      5) global any
    """
    farm = turbine.farm
    qs = ScadaUnitConfig.objects.all()

    def first_or_none(q):
        return q.order_by("-updated_at", "-created_at").first()

    cand = first_or_none(qs.filter(turbine=turbine, data_source=data_source))
    if cand:
        return cand
    cand = first_or_none(qs.filter(turbine=turbine, data_source="any"))
    if cand:
        return cand
    if farm:
        cand = first_or_none(qs.filter(farm=farm, turbine__isnull=True, data_source=data_source))
        if cand:
            return cand
        cand = first_or_none(qs.filter(farm=farm, turbine__isnull=True, data_source="any"))
        if cand:
            return cand
    cand = first_or_none(qs.filter(farm__isnull=True, turbine__isnull=True, data_source="any"))
    return cand


def resolve_unit_config(turbine: Turbines, data_source: str) -> UnitConfigResolved:
    cfg = _pick_unit_config(turbine, data_source)
    if not cfg:
        return UnitConfigResolved(
            data_source=data_source,
            active_power_unit="kW",
            wind_speed_unit="m/s",
            temperature_unit="K",
            pressure_unit="Pa",
            humidity_unit="ratio",
            config_id=None,
            config_scope="fallback_default",
        )

    scope = "global"
    if cfg.turbine_id:
        scope = f"turbine:{cfg.turbine_id}"
    elif cfg.farm_id:
        scope = f"farm:{cfg.farm_id}"

    return UnitConfigResolved(
        data_source=data_source,
        active_power_unit=cfg.active_power_unit,
        wind_speed_unit=cfg.wind_speed_unit,
        temperature_unit=cfg.temperature_unit,
        pressure_unit=cfg.pressure_unit,
        humidity_unit=cfg.humidity_unit,
        active_power_multiplier=float(cfg.active_power_multiplier or 1.0),
        wind_speed_multiplier=float(cfg.wind_speed_multiplier or 1.0),
        temperature_multiplier=float(cfg.temperature_multiplier or 1.0),
        pressure_multiplier=float(cfg.pressure_multiplier or 1.0),
        humidity_multiplier=float(cfg.humidity_multiplier or 1.0),
        config_id=cfg.id,
        config_scope=scope,
    )


def _to_numeric_series(s: pd.Series) -> pd.Series:
    # Preserve NaN; coerce non-numeric to NaN.
    return pd.to_numeric(s, errors="coerce")


def _convert_power_to_kw(power: pd.Series, unit: str) -> pd.Series:
    p = _to_numeric_series(power)
    if unit == "kW":
        return p
    if unit == "MW":
        return p * 1000.0
    if unit == "W":
        return p / 1000.0
    # Unknown unit: leave as-is (still numeric)
    return p


def _convert_wind_speed_to_ms(ws: pd.Series, unit: str) -> pd.Series:
    v = _to_numeric_series(ws)
    if unit == "m/s":
        return v
    if unit == "km/h":
        return v / 3.6
    return v


def _convert_temp_to_k(temp: pd.Series, unit: str) -> pd.Series:
    t = _to_numeric_series(temp)
    if unit == "K":
        return t
    if unit == "C":
        return t + 273.15
    return t


def _convert_pressure_to_pa(p: pd.Series, unit: str) -> Tuple[pd.Series, bool]:
    """
    Returns (pressure_pa, usable_in_density)
    """
    x = _to_numeric_series(p)
    if unit == "Pa":
        return x, True
    if unit in ("hPa", "mbar"):
        return x * 100.0, True
    if unit == "kPa":
        return x * 1000.0, True
    if unit == "bar":
        return x * 100000.0, True
    if unit in ("percent", "unknown"):
        return x, False
    return x, False


def _convert_humidity_to_ratio(h: pd.Series, unit: str) -> pd.Series:
    x = _to_numeric_series(h)
    if unit == "ratio":
        return x
    if unit == "percent":
        return x / 100.0
    return x


def normalize_scada_dataframe_units(
    df: pd.DataFrame,
    turbine: Turbines,
    data_source: str,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Normalize raw SCADA columns to canonical units expected by analytics/computation.

    Returns:
      - normalized df (copy)
      - units_meta dict (for API responses / traceability)
    """
    cfg = resolve_unit_config(turbine, data_source)
    out = df.copy()

    # Apply conversions only when columns exist.
    if "ACTIVE_POWER" in out.columns:
        out["ACTIVE_POWER"] = _convert_power_to_kw(out["ACTIVE_POWER"], cfg.active_power_unit) * cfg.active_power_multiplier
    if "WIND_SPEED" in out.columns:
        out["WIND_SPEED"] = _convert_wind_speed_to_ms(out["WIND_SPEED"], cfg.wind_speed_unit) * cfg.wind_speed_multiplier
    if "TEMPERATURE" in out.columns:
        out["TEMPERATURE"] = _convert_temp_to_k(out["TEMPERATURE"], cfg.temperature_unit) * cfg.temperature_multiplier
    pressure_usable = True
    if "PRESSURE" in out.columns:
        pa, pressure_usable = _convert_pressure_to_pa(out["PRESSURE"], cfg.pressure_unit)
        out["PRESSURE"] = pa * cfg.pressure_multiplier
        # If pressure isn't physical (percent/unknown), drop it so density() falls back to constants.
        if not pressure_usable:
            out = out.drop(columns=["PRESSURE"])
    if "HUMIDITY" in out.columns:
        out["HUMIDITY"] = _convert_humidity_to_ratio(out["HUMIDITY"], cfg.humidity_unit) * cfg.humidity_multiplier

    units_meta = {
        "canonical": CANONICAL_UNITS,
        "raw_config": {
            "data_source": cfg.data_source,
            "active_power_unit": cfg.active_power_unit,
            "wind_speed_unit": cfg.wind_speed_unit,
            "temperature_unit": cfg.temperature_unit,
            "pressure_unit": cfg.pressure_unit,
            "humidity_unit": cfg.humidity_unit,
            "active_power_multiplier": cfg.active_power_multiplier,
            "wind_speed_multiplier": cfg.wind_speed_multiplier,
            "temperature_multiplier": cfg.temperature_multiplier,
            "pressure_multiplier": cfg.pressure_multiplier,
            "humidity_multiplier": cfg.humidity_multiplier,
            "config_id": cfg.config_id,
            "config_scope": cfg.config_scope,
            "pressure_usable_in_density": pressure_usable,
        },
    }

    return out, units_meta

