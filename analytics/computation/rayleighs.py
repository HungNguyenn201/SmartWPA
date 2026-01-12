import numpy as np
from math import exp
import pandas as pd

scale = None
shape = None

def aep_bin(bin_ave, v_ave):
    return 1 - exp(-np.pi / 4 * (bin_ave / v_ave) ** 2)

def aep_weibull(bin_ave):
    return 1 - exp(-(bin_ave / scale) ** shape)

def rayleighs_aep(global_curve: dict, constants: dict, weibulls: dict) -> dict:
    obj = {}
    global scale
    scale = weibulls['scale']
    global shape
    shape = weibulls['shape']

    vi_bins = np.array(list(global_curve.keys()))
    pi_bins = np.array(list(global_curve.values()))
    vi_minus_1 = vi_bins - 0.5
    pi_minus_1 = pi_bins.copy()
    pi_minus_1[-1] = 0
    pi_minus_1 = np.roll(pi_minus_1, 1)

    for ave in range(4, 12):
        sum = 0
        for i in range(0, len(vi_bins)):
            sum += (aep_bin(vi_bins[i], ave) - aep_bin(vi_minus_1[i], ave)) * (pi_minus_1[i] + pi_bins[i]) / 2
        sum *= 8760
        obj[f'AepRayleighMeasured{ave}'] = sum

    v_cutout = constants['V_cutout']
    bin_step = vi_bins[1] - vi_bins[0]  # Step size between bins (typically 0.5 m/s)

    vi_bins_new = np.arange(vi_bins[0], v_cutout + bin_step, bin_step)
    # Handle alignment between vi_bins_new and pi_bins
    # Case 1: vi_bins_new is longer than pi_bins -> pad pi_bins with NaN
    # Case 2: vi_bins_new is shorter than pi_bins -> truncate pi_bins
    # Case 3: Same length -> keep as is
    
    if len(vi_bins_new) > len(pi_bins):
        # Pad pi_bins with NaN to match vi_bins_new length
        pad_length = len(vi_bins_new) - len(pi_bins)
        pi_bins = np.pad(pi_bins, (0, pad_length), mode='constant', constant_values=np.nan)
    elif len(vi_bins_new) < len(pi_bins):
        # Truncate pi_bins to match vi_bins_new length
        pi_bins = pi_bins[:len(vi_bins_new)]
    
    # Forward fill NaN values (extrapolate power curve to v_cutout)
    pi_bins = pd.Series(pi_bins).ffill().to_numpy()
    
    # Update vi_bins to the new range
    vi_bins = vi_bins_new
    vi_minus_1 = vi_bins - 0.5
    pi_minus_1 = pi_bins.copy()
    pi_minus_1[-1] = 0
    pi_minus_1 = np.roll(pi_minus_1, 1)
    for ave in range(4, 12):
        sum = 0
        for i in range(0, len(vi_bins)):
            sum += (aep_bin(vi_bins[i], ave) - aep_bin(vi_minus_1[i], ave)) * (pi_minus_1[i] + pi_bins[i]) / 2
        sum *= 8760
        obj[f'AepRayleighExtrapolated{ave}'] = sum

    sum = 0
    for i in range(0, len(vi_bins)):
        sum += (aep_weibull(vi_bins[i]) - aep_weibull(vi_minus_1[i])) * (pi_minus_1[i] + pi_bins[i]) / 2        
    obj['AepWeibullTurbine'] = 8760 * sum

    return obj