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
    vi_bins = np.arange(vi_bins[0], v_cutout + vi_bins[1] - vi_bins[0], vi_bins[1] - vi_bins[0])
    pi_bins = np.pad(pi_bins, (0, len(vi_bins) - len(pi_bins)), mode='constant', constant_values=np.nan)
    pi_bins = pd.Series(pi_bins).ffill().to_numpy()
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