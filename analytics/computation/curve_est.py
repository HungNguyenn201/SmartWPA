from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Literal

def get_power_curve(data: pd.DataFrame, groupby: Literal["global", "yearly", "quarterly", "monthly", "day/night"]="global") -> dict:
    output_obj = {}

    if (groupby == "global"):
        groups = data.groupby(data['status'], observed=True)
    elif (groupby == "yearly"):
        groups = data.groupby(pd.Grouper(freq='YS'), observed=True)
    elif (groupby == "quarterly"):
        groups = data.groupby(data.index.quarter, observed=True)
    elif (groupby == "monthly"):
        groups = data.groupby(data.index.month, observed=True)
    elif (groupby == "day/night"):
        hour = data.index.hour
        data['time_group'] = np.select(
            [hour >= 18, hour < 6],
            ['Night', 'Night'],
            default='Day'
        )
        groups = data.groupby(data['time_group'], observed=True)
    
    def get_key(groupby: Literal["global", "yearly", "quarterly", "monthly", "day/night"], group_key) -> str:
        if (groupby == "global"):
            return None
        elif (groupby == "yearly"):
            return str(group_key.year) if isinstance(group_key, pd.Timestamp) else str(group_key)
        elif (groupby == "quarterly"):
            return str(group_key)
        elif (groupby == "monthly"):
            return str(group_key)
        elif (groupby == "day/night"):
            return str(group_key).lower()

    for group_key, grps in groups:
        if (len(grps) == 0): continue
        grps = grps.copy()
        key = get_key(groupby, group_key)
        aggs = grps.groupby('bin', observed=True)['ACTIVE_POWER'].mean()

        if (key != None):
            output_obj[key] = aggs.to_dict()
        else:
            output_obj = aggs.to_dict()

    return output_obj

def get_all_power_curves(data: pd.DataFrame, air_density: pd.Series[float]) -> dict:
    obj = {}
    obj['global'] = get_power_curve(data)
    obj['yearly'] = get_power_curve(data, groupby='yearly')
    obj['quarterly'] = get_power_curve(data, groupby='quarterly')
    obj['monthly'] = get_power_curve(data, groupby='monthly')
    obj['day/night'] = get_power_curve(data, groupby='day/night')

    return obj