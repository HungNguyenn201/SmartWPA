"""
Pure computation helpers for cross-data analysis (turbine & windfarm level).

All numeric/config constants come from _header.CROSS_ANALYSIS_*.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from analytics.models import ClassificationPoint, Computation
from api_gateway.turbines_analysis.helpers._header import (
    to_epoch_ms,
    CROSS_ANALYSIS_DAY_NIGHT_NIGHT_END_HOUR,
    CROSS_ANALYSIS_DAY_NIGHT_NIGHT_START_HOUR,
    CROSS_ANALYSIS_MAX_POINTS_MAX,
    CROSS_ANALYSIS_MAX_POINTS_MIN,
    CROSS_ANALYSIS_SECTORS_NUMBER_DEFAULT,
    CROSS_ANALYSIS_SECTORS_NUMBER_MAX,
    CROSS_ANALYSIS_SECTORS_NUMBER_MIN,
    CROSS_ANALYSIS_STATUS_BY_CODE,
)
from api_gateway.turbines_analysis.helpers.computation_helper import load_turbine_data
from facilities.models import Turbines

logger = logging.getLogger("api_gateway.turbines_analysis")


# -----------------------------------------------------------------------------
# Sector / direction
# -----------------------------------------------------------------------------


def parse_sector_ids(sector_ids: List[int], sectors_number: int) -> List[int]:
    """Convert 1-based sector ids to 0-based if user sent 1..N."""
    if not sector_ids:
        return []
    if min(sector_ids) >= 1 and max(sector_ids) <= sectors_number:
        return [i - 1 for i in sector_ids]
    return list(sector_ids)


def compute_sector(direction_deg: np.ndarray, sectors_number: int) -> np.ndarray:
    """Map direction degrees [0, 360) to sector index in [0, sectors_number-1]."""
    d = np.mod(direction_deg, 360.0)
    frac = d / 360.0
    sec = np.floor(frac * float(sectors_number)).astype(int)
    return np.clip(sec, 0, sectors_number - 1)


# -----------------------------------------------------------------------------
# Regression
# -----------------------------------------------------------------------------

VALID_REGRESSION_TYPES = frozenset({
    "linear", "polynomial2", "polynomial3", "polynomial4",
    "exponential", "power", "logarithmic",
})

# Ordered list of regression types to return (client can select which to display).
REGRESSION_TYPES = [
    "linear", "polynomial2", "polynomial3", "polynomial4",
    "exponential", "power", "logarithmic",
]

# Default number of points to generate for each regression line (polyline).
# This is independent from scatter max_points (typically up to ~20k).
REGRESSION_LINE_POINTS_DEFAULT = 200


def _regression_metrics(y: np.ndarray, y_hat: np.ndarray) -> Tuple[Optional[float], float]:
    """Return (r2, rmse) from actual vs predicted."""
    resid = y - y_hat
    rmse = float(np.sqrt(np.mean(resid ** 2)))
    ss_res = float(np.sum(resid ** 2))
    ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
    r2 = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else None
    return r2, rmse


def _empty_regression(reg_type: str) -> Dict[str, Any]:
    return {"enabled": True, "type": reg_type, "coefficients": [], "equation": None, "r2": None, "rmse": None}


def linear_regression(
    x: np.ndarray, y: np.ndarray, force_zero_intercept: bool
) -> Dict[str, Any]:
    """Ordinary least squares; optional zero intercept."""
    if x.size < 2:
        return _empty_regression("linear")

    if force_zero_intercept:
        denom = float(np.dot(x, x))
        if denom == 0.0:
            return _empty_regression("linear")
        slope = float(np.dot(x, y) / denom)
        intercept = 0.0
    else:
        slope, intercept = np.polyfit(x, y, 1)
        slope = float(slope)
        intercept = float(intercept)

    y_hat = slope * x + intercept
    r2, rmse = _regression_metrics(y, y_hat)
    eq = f"y = {slope:.6g}*x + {intercept:.6g}" if intercept else f"y = {slope:.6g}*x"
    return {
        "enabled": True, "type": "linear",
        "coefficients": [slope, intercept],
        "equation": eq, "r2": r2, "rmse": rmse,
        "slope": slope, "intercept": intercept,
    }


def polynomial_regression(
    x: np.ndarray, y: np.ndarray, degree: int
) -> Dict[str, Any]:
    """Polynomial fit of given degree (2, 3, or 4)."""
    reg_type = f"polynomial{degree}"
    if x.size < degree + 1:
        return _empty_regression(reg_type)
    try:
        coeffs = np.polyfit(x, y, degree)
        y_hat = np.polyval(coeffs, x)
    except (np.linalg.LinAlgError, ValueError):
        return _empty_regression(reg_type)

    r2, rmse = _regression_metrics(y, y_hat)
    parts = []
    for i, c in enumerate(coeffs):
        power = degree - i
        if power > 1:
            parts.append(f"{c:.6g}*x^{power}")
        elif power == 1:
            parts.append(f"{c:.6g}*x")
        else:
            parts.append(f"{c:.6g}")
    eq = "y = " + " + ".join(parts)
    return {
        "enabled": True, "type": reg_type,
        "coefficients": [float(c) for c in coeffs],
        "equation": eq, "r2": r2, "rmse": rmse,
    }


def exponential_regression(x: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
    """Fit y = a * exp(b * x). Requires y > 0."""
    if x.size < 2:
        return _empty_regression("exponential")
    mask = y > 0
    if mask.sum() < 2:
        return _empty_regression("exponential")
    xf, yf = x[mask], y[mask]
    try:
        coeffs = np.polyfit(xf, np.log(yf), 1)
        b = float(coeffs[0])
        a = float(np.exp(coeffs[1]))
    except (np.linalg.LinAlgError, ValueError):
        return _empty_regression("exponential")

    y_hat = a * np.exp(b * x)
    r2, rmse = _regression_metrics(y, y_hat)
    return {
        "enabled": True, "type": "exponential",
        "coefficients": [a, b],
        "equation": f"y = {a:.6g}*exp({b:.6g}*x)",
        "r2": r2, "rmse": rmse,
    }


def power_regression(x: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
    """Fit y = a * x^b. Requires x > 0 and y > 0."""
    if x.size < 2:
        return _empty_regression("power")
    mask = (x > 0) & (y > 0)
    if mask.sum() < 2:
        return _empty_regression("power")
    xf, yf = x[mask], y[mask]
    try:
        coeffs = np.polyfit(np.log(xf), np.log(yf), 1)
        b = float(coeffs[0])
        a = float(np.exp(coeffs[1]))
    except (np.linalg.LinAlgError, ValueError):
        return _empty_regression("power")

    y_hat_full = np.where(x > 0, a * np.power(x, b), np.nan)
    valid = np.isfinite(y_hat_full) & np.isfinite(y)
    if valid.sum() < 2:
        return _empty_regression("power")
    r2, rmse = _regression_metrics(y[valid], y_hat_full[valid])
    return {
        "enabled": True, "type": "power",
        "coefficients": [a, b],
        "equation": f"y = {a:.6g}*x^{b:.6g}",
        "r2": r2, "rmse": rmse,
    }


def logarithmic_regression(x: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
    """Fit y = a * ln(x) + b. Requires x > 0."""
    if x.size < 2:
        return _empty_regression("logarithmic")
    mask = x > 0
    if mask.sum() < 2:
        return _empty_regression("logarithmic")
    xf, yf = x[mask], y[mask]
    try:
        coeffs = np.polyfit(np.log(xf), yf, 1)
        a = float(coeffs[0])
        b = float(coeffs[1])
    except (np.linalg.LinAlgError, ValueError):
        return _empty_regression("logarithmic")

    y_hat_full = np.where(x > 0, a * np.log(x) + b, np.nan)
    valid = np.isfinite(y_hat_full) & np.isfinite(y)
    if valid.sum() < 2:
        return _empty_regression("logarithmic")
    r2, rmse = _regression_metrics(y[valid], y_hat_full[valid])
    return {
        "enabled": True, "type": "logarithmic",
        "coefficients": [a, b],
        "equation": f"y = {a:.6g}*ln(x) + {b:.6g}",
        "r2": r2, "rmse": rmse,
    }


def compute_regression(
    x: np.ndarray,
    y: np.ndarray,
    reg_type: str = "linear",
    force_zero: bool = False,
) -> Dict[str, Any]:
    """Dispatcher: route to the correct regression function by type."""
    if reg_type == "linear":
        return linear_regression(x, y, force_zero)
    if reg_type in ("polynomial2", "polynomial3", "polynomial4"):
        degree = int(reg_type[-1])
        return polynomial_regression(x, y, degree)
    if reg_type == "exponential":
        return exponential_regression(x, y)
    if reg_type == "power":
        return power_regression(x, y)
    if reg_type == "logarithmic":
        return logarithmic_regression(x, y)
    return linear_regression(x, y, force_zero)


def _infer_x_domain(
    x: np.ndarray,
    reg_type: str,
    fallback_min: float = 0.0,
    fallback_max: float = 1.0,
) -> Tuple[float, float]:
    """Infer a safe x-domain [min, max] for generating regression line points."""
    if x.size == 0:
        return fallback_min, fallback_max

    x_min = float(np.nanmin(x))
    x_max = float(np.nanmax(x))
    if not np.isfinite(x_min) or not np.isfinite(x_max) or x_min == x_max:
        return fallback_min, fallback_max

    # Domain constraints
    if reg_type in ("logarithmic", "power"):
        pos = x[np.isfinite(x) & (x > 0)]
        if pos.size == 0:
            return fallback_min, fallback_max
        x_min = float(np.nanmin(pos))
        x_max = float(np.nanmax(pos))
        if not np.isfinite(x_min) or not np.isfinite(x_max) or x_min == x_max:
            return fallback_min, fallback_max

    return x_min, x_max


def _predict_y_hat(reg_obj: Dict[str, Any], x_line: np.ndarray) -> np.ndarray:
    """Predict y_hat for a regression object over x_line."""
    reg_type = (reg_obj.get("type") or "").lower()
    coeffs = reg_obj.get("coefficients") or []
    try:
        if reg_type == "linear" and len(coeffs) >= 2:
            a, b = float(coeffs[0]), float(coeffs[1])
            return a * x_line + b
        if reg_type in ("polynomial2", "polynomial3", "polynomial4") and len(coeffs) >= 3:
            c = np.array(coeffs, dtype=float)
            return np.polyval(c, x_line)
        if reg_type == "exponential" and len(coeffs) >= 2:
            a, b = float(coeffs[0]), float(coeffs[1])
            return a * np.exp(b * x_line)
        if reg_type == "power" and len(coeffs) >= 2:
            a, b = float(coeffs[0]), float(coeffs[1])
            return a * np.power(x_line, b)
        if reg_type == "logarithmic" and len(coeffs) >= 2:
            a, b = float(coeffs[0]), float(coeffs[1])
            return a * np.log(x_line) + b
    except Exception:
        pass
    return np.full_like(x_line, np.nan, dtype=float)


def _build_line_points(
    reg_obj: Dict[str, Any],
    x_domain: Tuple[float, float],
    n_points: int,
) -> List[Dict[str, float]]:
    """Generate line points [{x, y}] for a regression object."""
    if not reg_obj:
        return []
    coeffs = reg_obj.get("coefficients") or []
    if not coeffs:
        return []

    x_min, x_max = x_domain
    if not (np.isfinite(x_min) and np.isfinite(x_max)) or x_min == x_max:
        return []

    n = int(n_points) if n_points is not None else REGRESSION_LINE_POINTS_DEFAULT
    n = max(2, min(2000, n))
    x_line = np.linspace(x_min, x_max, n, dtype=float)

    reg_type = (reg_obj.get("type") or "").lower()
    if reg_type in ("logarithmic", "power"):
        x_line = x_line[x_line > 0]
        if x_line.size < 2:
            return []

    y_hat = _predict_y_hat(reg_obj, x_line)
    valid = np.isfinite(x_line) & np.isfinite(y_hat)
    if int(valid.sum()) < 2:
        return []

    xs = x_line[valid].tolist()
    ys = y_hat[valid].tolist()
    return [{"x": float(x), "y": float(y)} for x, y in zip(xs, ys)]


def compute_regressions_all_types(
    x: np.ndarray,
    y: np.ndarray,
    force_zero: bool = False,
    types: Optional[List[str]] = None,
    n_line_points: int = REGRESSION_LINE_POINTS_DEFAULT,
) -> Dict[str, Dict[str, Any]]:
    """Compute all regression types on (x, y). Returns dict keyed by type.

    Each regression object includes `line_points` (polyline) for drawing on FE.
    """
    if types is None:
        types = REGRESSION_TYPES
    out: Dict[str, Dict[str, Any]] = {}
    for reg_type in types:
        if reg_type not in VALID_REGRESSION_TYPES:
            continue
        reg_obj = compute_regression(x, y, reg_type=reg_type, force_zero=force_zero)
        x_domain = _infer_x_domain(x, reg_type)
        reg_obj["line_points"] = _build_line_points(reg_obj, x_domain, n_line_points)
        out[reg_type] = reg_obj
    return out


def compute_regressions_by_group(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    group_col: str,
    force_zero: bool = False,
    types: Optional[List[str]] = None,
    n_line_points: int = REGRESSION_LINE_POINTS_DEFAULT,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """For each group value, compute all regression types. Returns { group_label: { type: regression_obj } }."""
    if types is None:
        types = REGRESSION_TYPES
    if group_col not in df.columns or x_col not in df.columns or y_col not in df.columns:
        return {}
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for group_val, sub in df.groupby(group_col, dropna=False):
        label = str(group_val) if pd.notna(group_val) else "UNKNOWN"
        x = sub[x_col].to_numpy(dtype=float)
        y = sub[y_col].to_numpy(dtype=float)
        out[label] = compute_regressions_all_types(
            x, y, force_zero=force_zero, types=types, n_line_points=n_line_points
        )
    return out


# -----------------------------------------------------------------------------
# Cache key
# -----------------------------------------------------------------------------


def get_cross_analysis_cache_key(prefix: str, entity_id: int, payload: Dict[str, Any]) -> str:
    """Stable cache key from prefix, entity id, and normalized payload."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    h = hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{entity_id}_{h}"


# -----------------------------------------------------------------------------
# DataFrame preparation (normalize timestamp, select/rename columns)
# -----------------------------------------------------------------------------


def normalize_timestamp_ms(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has 'timestamp_ms' (milliseconds). Mutates df, returns it."""
    if "TIMESTAMP" not in df.columns:
        return df
    if pd.api.types.is_datetime64_any_dtype(df["TIMESTAMP"]):
        df["timestamp_ms"] = df["TIMESTAMP"].astype("int64") // 10**6
    else:
        df["timestamp_ms"] = df["TIMESTAMP"].astype("int64")
        df["timestamp_ms"] = df["timestamp_ms"].apply(lambda x: to_epoch_ms(x) if pd.notna(x) else None)
    return df


def select_and_rename_columns(
    df: pd.DataFrame,
    needed_sources: Set[str],
    source_to_field: Dict[str, str],
) -> pd.DataFrame:
    """Keep TIMESTAMP + needed fields; rename SCADA cols to source names. Returns subset df."""
    needed_fields = ["TIMESTAMP"]
    for src in needed_sources:
        field = source_to_field.get(src)
        if field:
            needed_fields.append(field)
    cols = [c for c in needed_fields if c in df.columns]
    out = df[cols].copy()
    rename = {source_to_field[s]: s for s in needed_sources if source_to_field.get(s) in out.columns}
    out.rename(columns=rename, inplace=True)
    return out


# -----------------------------------------------------------------------------
# Time-based columns and filters
# -----------------------------------------------------------------------------


def ensure_time_columns(df: pd.DataFrame) -> pd.Series:
    """Add _hour, _month, _year, _quarter from timestamp_ms. Returns ts_dt for reuse."""
    ts_dt = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["_hour"] = ts_dt.dt.hour
    df["_month"] = ts_dt.dt.month
    df["_year"] = ts_dt.dt.year
    df["_quarter"] = ts_dt.dt.quarter
    return ts_dt


def apply_time_filters(
    df: pd.DataFrame,
    start_hour: Optional[int],
    end_hour: Optional[int],
    months: List[int],
    day_night: str,
    night_start: int = CROSS_ANALYSIS_DAY_NIGHT_NIGHT_START_HOUR,
    night_end: int = CROSS_ANALYSIS_DAY_NIGHT_NIGHT_END_HOUR,
) -> pd.DataFrame:
    """Apply hour, month, day/night filters (manual 1.3.6.2.7 Advanced filters: Month, Day/Night). day_night in ('day'|'night'|'')."""
    if start_hour is not None:
        try:
            df = df[df["_hour"] >= int(start_hour)]
        except (TypeError, ValueError):
            pass
    if end_hour is not None:
        try:
            df = df[df["_hour"] <= int(end_hour)]
        except (TypeError, ValueError):
            pass
    if months:
        valid = [m for m in months if 1 <= int(m) <= 12]
        if valid:
            df = df[df["_month"].isin(valid)]
    if day_night == "night":
        is_night = (df["_hour"] >= night_start) | (df["_hour"] < night_end)
        df = df[is_night]
    elif day_night == "day":
        is_night = (df["_hour"] >= night_start) | (df["_hour"] < night_end)
        df = df[~is_night]
    return df


# -----------------------------------------------------------------------------
# Direction and range filters (manual 1.3.6.2.7 Advanced filters: Direction, Source)
# -----------------------------------------------------------------------------


def apply_direction_filter(
    df: pd.DataFrame,
    direction_source: str,
    sectors_number: int,
    sector_ids: List[int],
) -> pd.DataFrame:
    """Filter rows by direction sector. sector_ids 0-based; empty = no filter."""
    if direction_source not in df.columns or not sector_ids:
        return df
    dir_vals = pd.to_numeric(df[direction_source], errors="coerce").to_numpy(dtype=float)
    sec = compute_sector(dir_vals, sectors_number)
    df["_sector"] = sec
    return df[df["_sector"].isin(sector_ids)]


def apply_range_filters(df: pd.DataFrame, ranges: List[Dict[str, Any]]) -> pd.DataFrame:
    """Apply per-source min/max filters (manual 1.3.6.2.7 Advanced filters: Source)."""
    for r in ranges:
        src = r.get("source")
        if not src or src not in df.columns:
            continue
        col = pd.to_numeric(df[src], errors="coerce")
        vmin = r.get("min")
        vmax = r.get("max")
        if vmin is not None:
            try:
                df = df[col >= float(vmin)]
                col = pd.to_numeric(df[src], errors="coerce")
            except (TypeError, ValueError):
                pass
        if vmax is not None:
            try:
                df = df[col <= float(vmax)]
            except (TypeError, ValueError):
                pass
    return df


# -----------------------------------------------------------------------------
# X/Y columns and invalid drop
# -----------------------------------------------------------------------------


def build_xy_and_drop_invalid(
    df: pd.DataFrame, x_source: str, y_source: str
) -> pd.DataFrame:
    """Add x, y from sources; drop rows with NaN or non-finite."""
    df = df.copy()
    df["x"] = pd.to_numeric(df.get(x_source), errors="coerce")
    df["y"] = pd.to_numeric(df.get(y_source), errors="coerce")
    df = df.dropna(subset=["x", "y"])
    return df[np.isfinite(df["x"]) & np.isfinite(df["y"])]


# -----------------------------------------------------------------------------
# Classification merge (turbine / farm)
# -----------------------------------------------------------------------------


def _status_code_to_name(code: int, status_by_code: Tuple[str, ...]) -> str:
    if 0 <= code < len(status_by_code):
        return status_by_code[code]
    return "UNKNOWN"


def fetch_classification_for_turbine(
    turbine: Turbines,
    start_ms: Optional[int],
    end_ms: Optional[int],
    status_by_code: Tuple[str, ...] = CROSS_ANALYSIS_STATUS_BY_CODE,
) -> Optional[pd.DataFrame]:
    """Return DataFrame with columns timestamp_ms, group (status name)."""
    q = Computation.objects.filter(
        turbine=turbine, computation_type="classification", is_latest=True
    )
    if start_ms is not None and end_ms is not None:
        comp = q.filter(start_time=start_ms, end_time=end_ms).first()
    else:
        comp = q.order_by("-end_time").first()
    if not comp:
        return None
    cps = ClassificationPoint.objects.filter(computation=comp).only("timestamp", "classification")
    if start_ms is not None and end_ms is not None:
        cps = cps.filter(timestamp__gte=start_ms, timestamp__lte=end_ms)
    rows = list(cps.values_list("timestamp", "classification"))
    if not rows:
        return None
    cdf = pd.DataFrame(rows, columns=["timestamp_ms", "cls"])
    cdf["group"] = cdf["cls"].apply(lambda c: _status_code_to_name(int(c), status_by_code))
    return cdf[["timestamp_ms", "group"]]


def fetch_classification_for_farm(
    turbines: List[Turbines],
    start_ms: Optional[int],
    end_ms: Optional[int],
    status_by_code: Tuple[str, ...] = CROSS_ANALYSIS_STATUS_BY_CODE,
) -> pd.DataFrame:
    """Return DataFrame with columns timestamp_ms, turbine_id, group."""
    parts: List[pd.DataFrame] = []
    for t in turbines:
        cdf = fetch_classification_for_turbine(t, start_ms, end_ms, status_by_code)
        if cdf is not None and not cdf.empty:
            cdf = cdf.copy()
            cdf["turbine_id"] = t.id
            parts.append(cdf)
    if not parts:
        return pd.DataFrame(columns=["timestamp_ms", "turbine_id", "group"])
    return pd.concat(parts, ignore_index=True)


# -----------------------------------------------------------------------------
# Temporal group series (monthly, yearly, seasonally) — aligned with Power curve,
# Distribution, Speed analysis, Time profile: monthly/seasonally = profile (gom năm).
# -----------------------------------------------------------------------------


def get_temporal_group_series(
    df: pd.DataFrame, ts_dt: pd.Series, group_by: str
) -> Optional[pd.Series]:
    """Return group labels for each row.

    Semantics (aligned with power curve, distribution, speed analysis, time profile):
      - monthly: profile — gom tháng của tất cả năm (Jan, Feb, ...).
      - seasonally: profile — gom quý của tất cả năm (Q1, Q2, Q3, Q4).
      - yearly: time-series — theo từng năm (2012, 2013, ...).
      - time_profile_monthly / time_profile_seasonally: same as monthly/seasonally (profile).
    """
    idx = df.index
    if group_by == "monthly":
        # Profile: month of year, same as Distribution/Speed/Time profile
        return ts_dt.loc[idx].dt.strftime("%b")
    if group_by == "yearly":
        return ts_dt.loc[idx].dt.strftime("%Y")
    if group_by == "seasonally":
        # Profile: quarter of year (Q1–Q4), same as Distribution/Speed/Time profile
        return ts_dt.loc[idx].dt.quarter.map({1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"})
    if group_by == "time_profile_monthly":
        return ts_dt.loc[idx].dt.strftime("%b")
    if group_by == "time_profile_seasonally":
        return ts_dt.loc[idx].dt.quarter.map({1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"})
    return None


# -----------------------------------------------------------------------------
# Group by source (Z-axis binning)
# -----------------------------------------------------------------------------


def bin_source_values(
    df: pd.DataFrame,
    source_col: str,
    n_bins: int = 5,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
) -> Optional[pd.Series]:
    """Bin a continuous source column into n_bins equal-width bins. Returns group labels."""
    if source_col not in df.columns or df.empty:
        return None
    vals = pd.to_numeric(df[source_col], errors="coerce")
    lo = float(vmin) if vmin is not None else float(vals.min())
    hi = float(vmax) if vmax is not None else float(vals.max())
    if lo >= hi:
        return pd.Series("all", index=df.index)
    edges = np.linspace(lo, hi, n_bins + 1)
    labels = [f"{edges[i]:.1f}-{edges[i + 1]:.1f}" for i in range(n_bins)]
    return pd.cut(vals, bins=edges, labels=labels, include_lowest=True).astype(str)


# -----------------------------------------------------------------------------
# X/Y statistics (histogram + summary stats)
# -----------------------------------------------------------------------------


def compute_xy_statistics(
    df: pd.DataFrame, x_col: str = "x", y_col: str = "y", bins: int = 30
) -> Dict[str, Any]:
    """Compute histogram and basic stats for X and Y columns (before downsample)."""
    result: Dict[str, Any] = {}
    for col, label in [(x_col, "x"), (y_col, "y")]:
        vals = df[col].dropna()
        if vals.empty:
            result[f"{label}_histogram"] = []
            result[f"{label}_stats"] = {}
            continue
        arr = vals.to_numpy(dtype=float)
        counts, bin_edges = np.histogram(arr, bins=bins)
        centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])
        result[f"{label}_histogram"] = [
            {"bin_center": float(c), "count": int(n)} for c, n in zip(centers, counts)
        ]
        result[f"{label}_stats"] = {
            "mean": float(np.mean(arr)),
            "std": float(np.std(arr)),
            "min": float(np.min(arr)),
            "max": float(np.max(arr)),
            "median": float(np.median(arr)),
            "count": int(len(arr)),
        }
    return result


# -----------------------------------------------------------------------------
# Downsample and build output points
# -----------------------------------------------------------------------------


def clamp_max_points(value: Optional[int]) -> int:
    """Clamp to CROSS_ANALYSIS_MAX_POINTS_MIN .. MAX."""
    if value is None:
        from api_gateway.turbines_analysis.helpers._header import (
            CROSS_ANALYSIS_MAX_POINTS_DEFAULT,
        )
        return CROSS_ANALYSIS_MAX_POINTS_DEFAULT
    v = int(value)
    return max(CROSS_ANALYSIS_MAX_POINTS_MIN, min(CROSS_ANALYSIS_MAX_POINTS_MAX, v))


def downsample_to_max_points(df: pd.DataFrame, max_points: int) -> pd.DataFrame:
    """Reduce to max_points rows by uniform index sampling."""
    if len(df) <= max_points:
        return df
    idx = np.linspace(0, len(df) - 1, num=max_points).astype(int)
    return df.iloc[idx]


def build_points_list(
    df: pd.DataFrame,
    group_col: Optional[str] = "group",
    turbine_id_col: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build list of {timestamp_ms, x, y, group?, turbine_id?} for API."""
    need_group = group_col and group_col in df.columns
    need_turbine = turbine_id_col and turbine_id_col in df.columns
    cols = ["timestamp_ms", "x", "y"]
    if need_group:
        cols.append(group_col)
    if need_turbine:
        cols.append(turbine_id_col)
    cols = [c for c in cols if c in df.columns]
    out: List[Dict[str, Any]] = []
    for r in df[cols].itertuples(index=False):
        d: Dict[str, Any] = {
            "timestamp_ms": int(r.timestamp_ms),
            "x": float(r.x),
            "y": float(r.y),
            "group": None if not need_group else (None if pd.isna(getattr(r, group_col)) else str(getattr(r, group_col))),
        }
        if need_turbine:
            d["turbine_id"] = int(getattr(r, turbine_id_col))
        out.append(d)
    return out


# -----------------------------------------------------------------------------
# Farm-level data loading (concat all turbines with turbine_id)
# -----------------------------------------------------------------------------


def load_farm_scada_for_cross_analysis(
    turbines: List[Turbines],
    start_ms: Optional[int],
    end_ms: Optional[int],
    needed_sources: Set[str],
    source_to_field: Dict[str, str],
    max_points_per_turbine: Optional[int] = None,
) -> Tuple[Optional[pd.DataFrame], str, Dict[str, str]]:
    """
    Load SCADA for each turbine, concat with turbine_id. Normalize timestamp and rename columns.
    Returns (df_all, data_source_used, error_info). data_source_used is the last non-empty source;
    error_info keys are turbine ids.
    """
    if not turbines:
        return None, "db", {}

    needed_fields = ["TIMESTAMP"]
    for src in needed_sources:
        f = source_to_field.get(src)
        if f:
            needed_fields.append(f)

    parts: List[pd.DataFrame] = []
    data_source_used = "db"
    error_info: Dict[str, str] = {}

    use_classification_points = needed_sources.issubset({"power", "wind_speed"})

    for t in turbines:
        df_one = None
        src_used = None
        err: Dict[str, str] = {}
        if use_classification_points:
            try:
                comp_q = Computation.objects.filter(
                    turbine=t, computation_type="classification", is_latest=True
                )
                if start_ms is not None and end_ms is not None:
                    comp = comp_q.filter(start_time=start_ms, end_time=end_ms).first()
                    if comp is None:
                        comp = comp_q.order_by("-end_time").first()
                        if comp and not (int(comp.start_time) <= int(start_ms) and int(comp.end_time) >= int(end_ms)):
                            comp = None
                else:
                    comp = comp_q.order_by("-end_time").first()
                if comp is not None:
                    pts = ClassificationPoint.objects.filter(computation=comp).only(
                        "timestamp", "wind_speed", "active_power"
                    )
                    if start_ms is not None:
                        pts = pts.filter(timestamp__gte=int(start_ms))
                    if end_ms is not None:
                        pts = pts.filter(timestamp__lte=int(end_ms))
                    rows = list(pts.values_list("timestamp", "wind_speed", "active_power"))
                    if rows:
                        df_one = pd.DataFrame(rows, columns=["TIMESTAMP", "WIND_SPEED", "ACTIVE_POWER"])
                        src_used = "classification_points"
            except Exception as e:
                logger.debug("Farm cross-data: classification points fast-path failed for turbine %s: %s", t.id, str(e))

        if df_one is None:
            df_one, src_used, err, _units_meta = load_turbine_data(t, start_ms, end_ms, preferred_source="db")
        if df_one is None or df_one.empty:
            error_info[str(t.id)] = err.get("db", "") or err.get("file", "") or "No data"
            continue
        data_source_used = src_used or data_source_used
        df_one = select_and_rename_columns(df_one, needed_sources, source_to_field)
        if df_one.empty or "TIMESTAMP" not in df_one.columns:
            error_info[str(t.id)] = "Missing required columns"
            continue
        normalize_timestamp_ms(df_one)
        df_one["turbine_id"] = t.id
        if max_points_per_turbine and len(df_one) > max_points_per_turbine:
            df_one = downsample_to_max_points(df_one, max_points_per_turbine)
        parts.append(df_one)

    if not parts:
        return None, data_source_used, error_info

    out = pd.concat(parts, ignore_index=True)
    return out, data_source_used, error_info


def direction_filter_to_params(
    direction_filter: Dict[str, Any],
    valid_sources: Set[str],
) -> Tuple[str, int, List[int]]:
    """Return (direction_source, sectors_number, sector_ids). sector_ids 0-based."""
    direction_source = (direction_filter.get("source") or "").strip()
    if direction_source not in valid_sources:
        return "", 0, []
    n = int(direction_filter.get("sectors_number") or CROSS_ANALYSIS_SECTORS_NUMBER_DEFAULT)
    n = max(CROSS_ANALYSIS_SECTORS_NUMBER_MIN, min(CROSS_ANALYSIS_SECTORS_NUMBER_MAX, n))
    raw = direction_filter.get("sector_ids") or []
    try:
        sector_ids = [int(x) for x in raw]
    except (TypeError, ValueError):
        sector_ids = []
    sector_ids = parse_sector_ids(sector_ids, n)
    return direction_source, n, sector_ids


# -----------------------------------------------------------------------------
# Wind rose: sector aggregation when X axis = wind_direction
# -----------------------------------------------------------------------------


def compute_wind_rose_sectors(
    df: pd.DataFrame,
    sectors_number: int,
    direction_source: str = "wind_direction",
    y_col: str = "y",
    turbine_id_col: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
    """
    Aggregate df by direction sectors for wind rose output.
    df must have columns: direction (x or direction_source), y.
    Returns (sectors_list, by_turbine_list or None).
    Each sector: sector_id, start_deg, end_deg, center_deg, count, y_mean, y_min, y_max, y_median.
    If turbine_id_col is present and has multiple values, by_turbine_list is list of
    { turbine_id, turbine_name?, sectors: [...] }.
    """
    if df.empty or y_col not in df.columns:
        return [], None
    # Use x as direction (already in df from build_xy_and_drop_invalid when x_source=wind_direction)
    dir_col = "x" if "x" in df.columns else direction_source
    if dir_col not in df.columns:
        return [], None
    n = max(1, min(CROSS_ANALYSIS_SECTORS_NUMBER_MAX, int(sectors_number)))
    dir_vals = pd.to_numeric(df[dir_col], errors="coerce").to_numpy(dtype=float)
    sec = compute_sector(dir_vals, n)
    df = df.copy()
    df["_sector"] = sec
    y_vals = df["y"].to_numpy(dtype=float)

    def sector_bounds(sector_id: int) -> Tuple[float, float, float]:
        start_deg = (sector_id * 360.0) / n
        end_deg = ((sector_id + 1) * 360.0) / n
        center_deg = (sector_id + 0.5) * 360.0 / n
        return start_deg, end_deg, center_deg

    out_sectors: List[Dict[str, Any]] = []
    for sid in range(n):
        mask = df["_sector"] == sid
        count = int(mask.sum())
        start_deg, end_deg, center_deg = sector_bounds(sid)
        if count == 0:
            out_sectors.append({
                "sector_id": sid,
                "start_deg": round(start_deg, 2),
                "end_deg": round(end_deg, 2),
                "center_deg": round(center_deg, 2),
                "count": 0,
                "y_mean": None,
                "y_min": None,
                "y_max": None,
                "y_median": None,
            })
        else:
            ys = y_vals[mask]
            out_sectors.append({
                "sector_id": sid,
                "start_deg": round(start_deg, 2),
                "end_deg": round(end_deg, 2),
                "center_deg": round(center_deg, 2),
                "count": count,
                "y_mean": round(float(np.mean(ys)), 4),
                "y_min": round(float(np.min(ys)), 4),
                "y_max": round(float(np.max(ys)), 4),
                "y_median": round(float(np.median(ys)), 4),
            })

    by_turbine: Optional[List[Dict[str, Any]]] = None
    if turbine_id_col and turbine_id_col in df.columns:
        turb_ids = df[turbine_id_col].dropna().unique().tolist()
        if len(turb_ids) > 1:
            by_turbine = []
            for tid in turb_ids:
                sub = df[df[turbine_id_col] == tid]
                sub_sectors, _ = compute_wind_rose_sectors(
                    sub.rename(columns={}), sectors_number=n, direction_source=direction_source,
                    y_col=y_col, turbine_id_col=None,
                )
                by_turbine.append({"turbine_id": int(tid), "sectors": sub_sectors})
    return out_sectors, by_turbine
