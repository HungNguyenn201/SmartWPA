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
        groups = data.groupby(pd.Grouper(freq='QS'), observed=True)
    elif (groupby == "monthly"):
        groups = data.groupby(pd.Grouper(freq='MS'), observed=True)
    elif (groupby == "day/night"):
        hour = data.index.hour
        data['time_group'] = np.select(
            [hour >= 18, hour < 6],
            ['Night', 'Night'],
            default='Day'
        )
        groups = data.groupby(data['time_group'], observed=True)
    
    def get_key(groupby: Literal["global", "yearly", "quarterly", "monthly", "day/night"], timestamp=pd.Timestamp) -> str:
        if (groupby == "global"):
            return None
        elif (groupby == "yearly"):
            return timestamp.year
        elif (groupby == "quarterly"):
            return f"{timestamp.quarter}-{timestamp.year}"
        elif (groupby == "monthly"):
            return f"{timestamp.month}-{timestamp.year}"
        elif (groupby == "day/night"):
            if (timestamp.hour < 6 or timestamp.hour >= 18):
                return "night"
            return "day"

    for _, grps in groups:
        if (len(grps) == 0): continue
        grps = grps.copy()
        key = get_key(groupby, grps.iloc[0].name)
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