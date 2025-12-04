import pandas as pd
from classifier import classify, classification_to_obj
from curve_est import get_all_power_curves
from weibull import weibull
from indicators import indicators
from normalize import list_len, check_column_names, preprocess, verify_normal, normalize_data
from density import air_density
from bins import binning
from capacity_factor import capacity_factor
from rayleighs import rayleighs_aep
import concurrent.futures
import os

def start_time(data: pd.DataFrame) -> int:
    return data.iloc[0].name.timestamp()

def end_time(data: pd.DataFrame) -> int:
    return data.iloc[-1].name.timestamp()

def process(data: pd.Dataframe, constants: dict) -> dict:
    obj = {}
        
    classified = classify(data, constants)
    
    normals = classified[classified['status'] == 'NORMAL'].copy()
    verify_normal(normals, constants)

    air_densities = air_density(normals)
    binned_normalized = binning(normalize_data(normals, air_densities))

    power_curves = get_all_power_curves(binned_normalized, air_densities)
    obj['power_curves'] = power_curves

    weibulls = weibull(normals)

    obj['indicators'] = {}
    obj['indicators'].update(rayleighs_aep(power_curves['global'], constants, weibulls))
    obj['indicators'].update(indicators(classified, constants))
    obj['indicators']['CapacityFactor'] = capacity_factor(binned_normalized, constants)

    tasks = [
        ('start_time', start_time, (data,)),
        ('end_time', end_time, (data,)),
        ('classification', classification_to_obj, (classified,))
    ]

    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(func, *args): key for key, func, args in tasks}

        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            result = future.result()
            obj[key] = result

    return obj

required_constants = [
    'V_cutin', 'V_cutout', 'V_rated', 'P_rated', 'Swept_area'
]

def check_constants(constants: dict):
    keys = constants.keys()
    for constant in required_constants:
        if constant not in keys:
            raise ValueError(f"Constant {constant} not found.")


def check_data_integrity(constants: dict, data: pd.Dataframe = None, names: list[str] = None, lists: list[list[any]] = None):
    check_constants(constants)
    if (names != None):
        if not list_len(lists):
            raise ValueError("Not all lists have a constant length.")
    
    check_column_names(data.columns)


def get_wpa(data: pd.DataFrame, constants: dict) -> dict:
    check_data_integrity(constants, data=data)

    data = preprocess(data)

    return process(data, constants)

def get_wpa_lists(names: list[str], lists: list[list[any]], constants: dict) -> dict:
    check_data_integrity(constants, names=names, lists=lists)
    
    data = pd.DataFrame(lists, columns=names)
    data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

    data = preprocess(data)
    
    return process(data, constants)