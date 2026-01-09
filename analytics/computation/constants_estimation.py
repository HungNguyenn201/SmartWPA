from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ConstantEstimationConfig:
    """
    IEC-inspired config for estimating turbine operating constants from SCADA.

    Recommended (practical WPA defaults):
    - bin_width = 0.5 m/s
    - min_samples_per_bin = 30  (10-min SCADA => >= 5 hours)
    - cutin_alpha = 0.05 * P_rated
    - zero_power_alpha = 0.02 * P_rated
    - rated_alpha = 0.98 * P_rated
    """

    bin_width: float = 0.5
    min_samples_per_bin: int = 30

    # Thresholds expressed as a fraction of P_rated
    cutin_alpha: float = 0.05
    zero_power_alpha: float = 0.02
    rated_alpha: float = 0.98

    # Cut-out bin rules
    cutout_power_alpha: float = 0.2
    cutout_zero_ratio: float = 0.7

    # P_rated robustness (avoid single spikes)
    prated_top_fraction: float = 0.005  # top 0.5% points
    prated_min_points: int = 20


def _basic_outlier_filter(
    data: pd.DataFrame,
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
) -> pd.DataFrame:
    """
    Remove obvious outliers before constant estimation (IEC-inspired).
    
    IEC 61400-12-1 guidelines + practical WPA rules:
    - Wind speed: [0, 32] m/s (32 m/s ~ extreme survival speed for most turbines)
    - Power: reasonable range before knowing P_rated (eliminate sensor errors)
    
    This is a lightweight pre-filter; full classification happens later.
    """
    df = data.copy()
    
    # Filter obvious wind speed outliers (IEC: valid meteorological range)
    if wind_col in df.columns:
        df = df[(df[wind_col] >= 0) & (df[wind_col] <= 32)]
    
    # Filter obvious power outliers (before knowing P_rated)
    # Most turbines: P_rated < 10 MW = 10000 kW
    # Allow some negative for grid losses: -500 kW reasonable threshold
    if power_col in df.columns:
        df = df[(df[power_col] >= -500) & (df[power_col] <= 10000)]
    
    return df


def _iec_bin_centers(max_ws: float, bin_width: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    Match existing IEC-style binning in this repo (`analytics/computation/bins.py`):
    edges start at 0.25 and label centers at 0.5, 1.0, ...
    """
    start_edge = bin_width / 2.0  # 0.25 for 0.5 m/s
    edges = np.arange(start_edge, max_ws + bin_width, bin_width)
    labels = edges[:-1] + (bin_width / 2.0)
    return edges, labels


def estimate_p_rated(data: pd.DataFrame, power_col: str = "ACTIVE_POWER", cfg: ConstantEstimationConfig = ConstantEstimationConfig()) -> float:
    """
    Estimate rated power from SCADA robustly (IEC-inspired).

    Formula (practical WPA):
      P_rated ≈ median( top 0.5% of non-negative power points )
    
    Rationale: median of top fraction reduces sensitivity to spikes/outliers.
    """
    s = data[power_col]
    s = s[(s.notna()) & (s >= 0)]
    if s.empty:
        raise ValueError("Cannot estimate P_rated: no valid non-negative power points.")

    top_n = max(cfg.prated_min_points, int(np.ceil(len(s) * cfg.prated_top_fraction)))
    top = s.nlargest(min(top_n, len(s)))
    return float(top.median())


def _bin_stats(
    data: pd.DataFrame,
    bin_width: float,
    wind_col: str,
    power_col: str,
    p_rated: float,
    cfg: ConstantEstimationConfig,
) -> pd.DataFrame:
    """
    Return per-bin stats needed for cut-in / rated / cut-out inference.
    """
    df = data[[wind_col, power_col]].copy()
    df = df.dropna(subset=[wind_col, power_col])
    if df.empty:
        return pd.DataFrame(columns=["bin", "n", "p_mean", "zero_ratio"])

    edges, labels = _iec_bin_centers(float(df[wind_col].max()), bin_width)
    df["bin"] = pd.cut(df[wind_col], bins=edges, labels=labels, include_lowest=True, right=True)
    df = df.dropna(subset=["bin"])
    if df.empty:
        return pd.DataFrame(columns=["bin", "n", "p_mean", "zero_ratio"])

    df["bin"] = df["bin"].astype(float)
    zero_thr = cfg.zero_power_alpha * p_rated
    df["_is_zero"] = df[power_col] < zero_thr

    g = df.groupby("bin", observed=True)
    out = pd.DataFrame(
        {
            "bin": g.size().index.astype(float),
            "n": g.size().to_numpy(),
            "p_mean": g[power_col].mean().to_numpy(),
            "zero_ratio": g["_is_zero"].mean().to_numpy(),
        }
    )
    out = out.sort_values("bin").reset_index(drop=True)
    return out


def estimate_v_rated(
    data: pd.DataFrame,
    p_rated: float,
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
    cfg: ConstantEstimationConfig = ConstantEstimationConfig(),
) -> float:
    """
    Estimate v_rated from bins:
    Find the smallest bin center where mean power reaches rated_alpha * P_rated.
    """
    df = data[(data[power_col].notna()) & (data[wind_col].notna()) & (data[power_col] >= 0)]
    stats = _bin_stats(df, cfg.bin_width, wind_col, power_col, p_rated, cfg)
    if stats.empty:
        raise ValueError("Cannot estimate V_rated: insufficient valid data after filtering.")

    thr = cfg.rated_alpha * p_rated
    eligible = stats[(stats["n"] >= cfg.min_samples_per_bin) & (stats["p_mean"] >= thr)]
    if not eligible.empty:
        return float(eligible.iloc[0]["bin"])

    # Fallback: take bin at max mean power (still gives a plausible knee)
    idx = int(stats["p_mean"].idxmax())
    return float(stats.loc[idx, "bin"])


def estimate_v_cutin_iec_binning(
    data: pd.DataFrame,
    p_rated: float,
    v_rated: float,
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
    cfg: ConstantEstimationConfig = ConstantEstimationConfig(),
) -> Tuple[Optional[float], pd.DataFrame]:
    """
    IEC bin-based cut-in estimation (per user-provided spec).

    Step 1 (IEC binning): bin_width = 0.5 m/s.
    Consider only:
      - power >= 0
      - wind_speed < v_rated

    Step 2 + 3 (stable power threshold):
      A bin i is "cut-in" if:
        P̄_i > α * P_rated   and   N_i >= N_min
      where α = 0.03~0.05 (WPA often uses 0.05), N_min >= 30 points.

    Summary formula:
      v_cut-in = arg min_{v_i} ( P̄_i > 0.05 * P_rated  ∧  N_i >= N_min )
    """
    df = data[(data[power_col].notna()) & (data[wind_col].notna())]
    df = df[(df[power_col] >= 0) & (df[wind_col] < v_rated)]

    stats = _bin_stats(df, cfg.bin_width, wind_col, power_col, p_rated, cfg)
    if stats.empty:
        return None, stats

    thr = cfg.cutin_alpha * p_rated
    eligible = stats[(stats["n"] >= cfg.min_samples_per_bin) & (stats["p_mean"] > thr)]
    if eligible.empty:
        return None, stats
    return float(eligible.iloc[0]["bin"]), stats


def estimate_v_cutout_iec_binning(
    data: pd.DataFrame,
    p_rated: float,
    v_rated: float,
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
    cfg: ConstantEstimationConfig = ConstantEstimationConfig(),
) -> Tuple[Optional[float], pd.DataFrame]:
    """
    Practical IEC-inspired cut-out estimation with hysteresis-awareness (shutdown ≠ power==0 everywhere).

    Step 1: Identify "rated region" via v_rated (provided).
    Step 2: For bins with v_i > v_rated, find the first bin satisfying:
      P̄_i < 0.2 * P_rated
    Step 3: Confirm it's shutdown (not curtailment) by checking near-zero dominance:
      #(P < 0.02 * P_rated) / N_i > 70%

    Summary formula (your note):
      v_cut-out = arg min_{v_i > v_rated} ( P̄_i < 0.2 * P_rated )
    With additional shutdown confirmation:
      #(P < 0.02 * P_rated)/N_i > 0.70
    """
    df = data[(data[power_col].notna()) & (data[wind_col].notna())]
    df = df[(df[power_col] >= 0) & (df[wind_col] > v_rated)]

    stats = _bin_stats(df, cfg.bin_width, wind_col, power_col, p_rated, cfg)
    if stats.empty:
        return None, stats

    eligible = stats[
        (stats["n"] >= cfg.min_samples_per_bin)
        & (stats["p_mean"] < cfg.cutout_power_alpha * p_rated)
        & (stats["zero_ratio"] > cfg.cutout_zero_ratio)
    ]
    if eligible.empty:
        return None, stats
    return float(eligible.iloc[0]["bin"]), stats


def derive_turbine_constants_from_scada(
    data: pd.DataFrame,
    base_constants: Optional[Dict[str, Any]] = None,
    cfg: ConstantEstimationConfig = ConstantEstimationConfig(),
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
    include_debug: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Derive per-dataset constants from SCADA (IEC 61400-12-1 inspired).
    
    Workflow:
    1. Basic outlier filtering (wind: [0,32] m/s, power: reasonable range)
    2. Estimate P_rated from top percentile
    3. Estimate V_rated (bin where P̄ >= 0.98 * P_rated)
    4. Estimate V_cutin (first bin: P̄ > 0.05 * P_rated, N >= 30, wind < V_rated)
    5. Estimate V_cutout (first bin: wind > V_rated, P̄ < 0.2 * P_rated, zero_ratio > 70%)

    Returns: (constants, debug)
    - constants: dict with V_cutin, V_cutout, V_rated, P_rated, + base_constants
    - debug: per-step artifacts (bin tables) when include_debug=True
    """
    base_constants = dict(base_constants or {})

    # Step 1: Remove obvious outliers (IEC data quality check)
    data = _basic_outlier_filter(data, wind_col=wind_col, power_col=power_col)
    if data.empty:
        raise ValueError("No valid data after basic outlier filtering.")

    # Step 2-3: Estimate rated power and rated wind speed
    # Formula: P_rated = median(top 0.5% power)
    #          V_rated = arg min_v ( P̄(v) >= 0.98 * P_rated )
    p_rated = estimate_p_rated(data, power_col=power_col, cfg=cfg)
    v_rated = estimate_v_rated(data, p_rated=p_rated, wind_col=wind_col, power_col=power_col, cfg=cfg)

    # Step 4: Cut-in estimation (IEC bin-based method)
    # Formula: v_cut-in = arg min_{v_i} ( P̄_i > 0.05·P_rated  ∧  N_i >= 30 )
    v_cutin, cutin_bins = estimate_v_cutin_iec_binning(
        data, p_rated=p_rated, v_rated=v_rated, wind_col=wind_col, power_col=power_col, cfg=cfg
    )
    
    # Step 5: Cut-out estimation (IEC hysteresis-aware method)
    # Formula: v_cut-out = arg min_{v_i > v_rated} ( P̄_i < 0.2·P_rated  ∧  zero_ratio > 0.7 )
    v_cutout, cutout_bins = estimate_v_cutout_iec_binning(
        data, p_rated=p_rated, v_rated=v_rated, wind_col=wind_col, power_col=power_col, cfg=cfg
    )

    constants = dict(base_constants)
    constants.update(
        {
            "P_rated": float(p_rated),
            "V_rated": float(v_rated),
        }
    )
    if v_cutin is not None:
        constants["V_cutin"] = float(v_cutin)
    if v_cutout is not None:
        constants["V_cutout"] = float(v_cutout)

    if not include_debug:
        return constants, {}

    debug = {
        "estimated": {
            "P_rated": float(p_rated),
            "V_rated": float(v_rated),
            "V_cutin": None if v_cutin is None else float(v_cutin),
            "V_cutout": None if v_cutout is None else float(v_cutout),
        },
        "binning": {
            "bin_width": cfg.bin_width,
            "min_samples_per_bin": cfg.min_samples_per_bin,
            "cutin_bins": cutin_bins.to_dict(orient="records"),
            "cutout_bins": cutout_bins.to_dict(orient="records"),
        },
        "thresholds": {
            "cutin_alpha": cfg.cutin_alpha,
            "rated_alpha": cfg.rated_alpha,
            "cutout_power_alpha": cfg.cutout_power_alpha,
            "zero_power_alpha": cfg.zero_power_alpha,
            "cutout_zero_ratio": cfg.cutout_zero_ratio,
        },
    }

    return constants, debug


