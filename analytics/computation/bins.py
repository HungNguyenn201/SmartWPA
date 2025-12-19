import numpy as np
import pandas as pd

def binning(data: pd.DataFrame, bin_width=0.5) -> pd.DataFrame:
    bins = np.arange(0.25, data['WIND_SPEED'].max() + bin_width, bin_width)
    data['bin'] = pd.cut(
        data['WIND_SPEED'], 
        bins=bins,
        include_lowest=True,
        right=True
    )

    labels = bins[:-1] + (bin_width / 2)

    data['bin'] = pd.cut(
        data['WIND_SPEED'], 
        bins=bins, 
        labels=labels, 
        include_lowest=True, 
        right=True
    )
    
    data['bin'] = data['bin'].astype(float)

    return data