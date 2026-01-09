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


def estimate_v_cutin_timeseries(
    data: pd.DataFrame,
    p_rated: float,
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
    cfg: ConstantEstimationConfig = ConstantEstimationConfig(),
    consecutive: int = 3,
) -> Optional[float]:
    """
    Method 3 - Time-series based cut-in estimation (Meteodyn-like).
    
    Applies when SCADA data is long enough (≥ 6 months).
    
    Algorithm:
    1. Find transition events: wind ↑ & power: 0 → > threshold
    2. Conditions:
       - power > 0.05 * P_rated
       - maintained for ≥ 3 consecutive samples
    3. Formula: v_cut-in = median(v_transition)
    
    Formula:
      v_cut-in = median(v_transition)
      where v_transition = {v_i | power_{i-1} <= threshold AND power_i > threshold 
                            AND power stays > threshold for >= consecutive samples}
    """
    df = data[[wind_col, power_col]].copy()
    df = df.dropna()
    if df.empty:
        return None
    
    # Ensure data is sorted by timestamp (if TIMESTAMP exists) or by index
    if "TIMESTAMP" in df.columns:
        df = df.sort_values("TIMESTAMP")
    else:
        df = df.sort_index()
    
    thr = cfg.cutin_alpha * p_rated
    zero_thr = cfg.zero_power_alpha * p_rated  # ~0.02 * P_rated for "near zero"
    ws = df[wind_col].values
    pw = df[power_col].values
    
    transition_wind_speeds = []
    
    # Find transitions: wind ↑ & power: 0 → > threshold
    # Per Meteodyn spec:
    # - power_{i-1} near 0 (< zero_thr) AND power_i > thr
    # - wind is increasing (ws[i] > ws[i-1])
    # - power stays > thr for >= consecutive samples
    for i in range(1, len(df)):
        # Check if power transitions from near-zero to above threshold
        if pw[i - 1] < zero_thr and pw[i] > thr:
            # Check if wind is increasing
            if ws[i] > ws[i - 1]:
                # Check if power stays > threshold for at least 'consecutive' samples
                if i + consecutive <= len(pw):
                    if np.all(pw[i : i + consecutive] > thr):
                        transition_wind_speeds.append(float(ws[i]))
    
    if not transition_wind_speeds:
        return None
    
    return float(np.median(transition_wind_speeds))


def estimate_v_cutout_timeseries(
    data: pd.DataFrame,
    p_rated: float,
    v_rated: float,
    wind_col: str = "WIND_SPEED",
    power_col: str = "ACTIVE_POWER",
    cfg: ConstantEstimationConfig = ConstantEstimationConfig(),
    max_samples_to_zero: int = 2,
) -> Optional[float]:
    """
    Method 3 - Time-series based cut-out estimation (Meteodyn-like).
    
    Applies when SCADA data is long enough (≥ 6 months).
    
    Algorithm:
    1. Find shutdown events: wind ↑ & power: rated → 0
    2. Conditions:
       - v > v_rated
       - power drops to ~0 within ≤ 2 samples
    3. Formula: v_cut-out = median(v_shutdown)
    
    Formula:
      v_cut-out = median(v_shutdown)
      where v_shutdown = {v_i | v_i > v_rated AND power_{i-k} >= rated_alpha * P_rated 
                           AND power drops to ~0 within <= max_samples_to_zero samples}
    """
    df = data[[wind_col, power_col]].copy()
    df = df.dropna()
    if df.empty:
        return None
    
    # Ensure data is sorted by timestamp (if TIMESTAMP exists) or by index
    if "TIMESTAMP" in df.columns:
        df = df.sort_values("TIMESTAMP")
    else:
        df = df.sort_index()
    
    ws = df[wind_col].values
    pw = df[power_col].values
    
    # Thresholds
    rated_thr = cfg.rated_alpha * p_rated  # ~0.98 * P_rated
    zero_thr = cfg.zero_power_alpha * p_rated  # ~0.02 * P_rated
    
    shutdown_wind_speeds = []
    
    # Find shutdown events: wind ↑ & power: rated → 0
    # Per Meteodyn spec:
    # - v > v_rated
    # - power drops from rated (>= rated_thr) to ~0 within ≤ max_samples_to_zero samples
    # - "wind ↑" means wind is in high-wind region (not necessarily increasing every sample)
    for i in range(max_samples_to_zero + 1, len(df)):
        # Wind must be > v_rated (high-wind region)
        if ws[i] <= v_rated:
            continue
        
        # Current power should be near zero
        if pw[i] > zero_thr:
            continue
        
        # Look back to find if power was at rated level recently
        # and dropped to zero within max_samples_to_zero samples
        found_rated_idx = None
        
        # Look back up to max_samples_to_zero samples
        for j in range(i - 1, max(0, i - max_samples_to_zero - 1), -1):
            if pw[j] >= rated_thr:
                found_rated_idx = j
                break
        
        if found_rated_idx is not None:
            # Check that power dropped from rated to near-zero within max_samples_to_zero samples
            samples_to_drop = i - found_rated_idx
            if samples_to_drop <= max_samples_to_zero:
                # Additional check: wind should be relatively high throughout the drop
                # (to distinguish from low-wind stops)
                wind_window = ws[found_rated_idx:i + 1]
                if np.all(wind_window > v_rated * 0.9):  # Allow some margin
                    shutdown_wind_speeds.append(float(ws[i]))
    
    if not shutdown_wind_speeds:
        return None
    
    return float(np.median(shutdown_wind_speeds))


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
    4. Estimate V_cutin:
       - Method 3 (Time-series): if data ≥ 6 months, use transition-based median
       - Method 1 (IEC bin-based): fallback or if data < 6 months
    5. Estimate V_cutout:
       - Method 3 (Time-series): if data ≥ 6 months, use shutdown-event median
       - Method 1 (IEC bin-based): fallback or if data < 6 months

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

    # Check if data is long enough for Method 3 (≥ 6 months)
    # Assuming 10-minute SCADA: 6 months ≈ 6 * 30 * 24 * 6 = 25,920 samples
    # Use a more lenient threshold: ~4 months ≈ 17,000 samples
    use_timeseries_method = False
    if "TIMESTAMP" in data.columns:
        try:
            data_sorted = data.sort_values("TIMESTAMP")
            time_span = (data_sorted["TIMESTAMP"].iloc[-1] - data_sorted["TIMESTAMP"].iloc[0])
            # Check if span is ≥ 4 months (more lenient than 6 months requirement)
            if time_span.days >= 120:  # ~4 months
                use_timeseries_method = True
        except Exception:
            pass
    
    # If no timestamp, use sample count as proxy (assuming 10-min SCADA)
    if not use_timeseries_method and len(data) >= 17000:  # ~4 months of 10-min data
        use_timeseries_method = True

    # Step 4: Cut-in estimation
    # Try Method 3 (Time-series) first if data is long enough
    v_cutin = None
    cutin_bins = pd.DataFrame()
    if use_timeseries_method:
        v_cutin = estimate_v_cutin_timeseries(
            data, p_rated=p_rated, wind_col=wind_col, power_col=power_col, cfg=cfg, consecutive=3
        )
    
    # Fallback to Method 1 (IEC bin-based) if Method 3 didn't work
    if v_cutin is None:
        v_cutin, cutin_bins = estimate_v_cutin_iec_binning(
            data, p_rated=p_rated, v_rated=v_rated, wind_col=wind_col, power_col=power_col, cfg=cfg
        )
    
    # Step 5: Cut-out estimation
    # Try Method 3 (Time-series) first if data is long enough
    v_cutout = None
    cutout_bins = pd.DataFrame()
    if use_timeseries_method:
        v_cutout = estimate_v_cutout_timeseries(
            data, p_rated=p_rated, v_rated=v_rated, wind_col=wind_col, power_col=power_col, cfg=cfg, max_samples_to_zero=2
        )
    
    # Fallback to Method 1 (IEC bin-based) if Method 3 didn't work
    if v_cutout is None:
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

    # ------------------------------------------------------------------
    # Fallbacks (practical necessity)
    #
    # Some datasets do not contain enough high-wind shutdown points to detect
    # cut-out hysteresis reliably. However, the downstream pipeline requires
    # V_cutout to be present (classifier filtering + coverage checks).
    #
    # Strategy:
    # - If V_cutout cannot be detected, fall back to the maximum observed wind
    #   speed (floored to bin width) but never below V_rated.
    #
    # This keeps V_cutout consistent with the dataset coverage and avoids
    # creating a huge number of empty bins in later coverage checks.
    # ------------------------------------------------------------------
    if v_cutin is None:
        # Soft fallback: first bin with mean power above threshold, ignoring N_min.
        try:
            thr = cfg.cutin_alpha * p_rated
            soft = cutin_bins[(cutin_bins["n"] > 0) & (cutin_bins["p_mean"] > thr)]
            if not soft.empty:
                v_cutin = float(soft.iloc[0]["bin"])
        except Exception:
            v_cutin = None

    if v_cutout is None:
        ws = data[wind_col].dropna()
        if not ws.empty:
            max_ws = float(ws.max())
            # Floor to bin width to avoid exceeding observed coverage
            floored = float(np.floor(max_ws / cfg.bin_width) * cfg.bin_width)
            v_cutout = float(max(v_rated, floored))

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


