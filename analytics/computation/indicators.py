import pandas as pd
from .estimate import estimate
from .yaw_error import yaw_errors

def indicators(classified: pd.DataFrame, constants: dict) -> dict:
    obj = {}
    estimated_data = estimate(classified, fill_flag=True)
    resolution = estimated_data.iloc[1].name - estimated_data.iloc[0].name

    obj['AverageWindSpeed'] = estimated_data['WIND_SPEED'].mean()
    obj['ReachableEnergy'] = estimated_data['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['RealEnergy'] = estimated_data['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['LossEnergy'] = obj['ReachableEnergy'] - obj['RealEnergy']
    obj['LossPercent'] = obj['LossEnergy'] / obj['ReachableEnergy']
    
    tmp = (estimated_data.groupby(pd.Grouper(freq='D'))['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))) \
                        .rename_axis('date') \
                        .to_frame('DailyProduction') \
                        .reset_index()
                            
    tmp['date'] = tmp['date'].dt.strftime('%Y-%m-%d')
    obj['DailyProduction'] = tmp.to_dict(orient='records')
    
    
    obj['RatedPower'] = constants['P_rated']
    R = len(estimated_data[(estimated_data['status'] == 'NORMAL') 
                                | (estimated_data['status'] == 'CURTAILMENT') 
                                | (estimated_data['status'] == 'PARTIAL_CURTAILMENT') 
                                | (estimated_data['status'] == 'OVERPRODUCTION') 
                                | (estimated_data['status'] == 'UNDERPRODUCTION')])
    U = len(estimated_data[(estimated_data['status'] == 'STOP') 
                                | (estimated_data['status'] == 'PARTIAL_STOP')])
    obj['Tba'] = R / (R + U)

    non_errors = estimated_data[estimated_data['status'] != 'MEASUREMENT_ERROR']
    obj['Pba'] = non_errors['ACTIVE_POWER'].sum() / non_errors['ESTIMATED_POWER'].sum()
    
    tmp = estimated_data[estimated_data['status'] == 'STOP']
    obj['StopLoss'] = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1)) \
                    - tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))

    tmp = estimated_data[estimated_data['status'] == 'PARTIAL_STOP']
    obj['PartialStopLoss'] = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1)) \
                    - tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))

    tmp = estimated_data[estimated_data['status'] == 'UNDERPRODUCTION']
    obj['UnderProductionLoss'] = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1)) \
                    - tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))

    tmp = estimated_data[estimated_data['status'] == 'CURTAILMENT']
    obj['CurtailmentLoss'] = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1)) \
                    - tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))

    tmp = estimated_data[estimated_data['status'] == 'PARTIAL_CURTAILMENT']
    obj['PartialCurtailmentLoss'] = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1)) \
                    - tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))

    obj['TotalStopPoints'] = len(classified[classified['status'] == 'STOP'])
    obj['TotalPartialStopPoints'] = len(classified[classified['status'] == 'PARTIAL_STOP'])
    obj['TotalUnderProductionPoints'] = len(classified[classified['status'] == 'UNDERPRODUCTION'])
    obj['TotalCurtailmentPoints'] = len(classified[classified['status'] == 'CURTAILMENT'])
    obj['TimeStep'] = resolution.total_seconds()
    obj['TotalDuration'] = (classified.index.max() - classified.index.min()).total_seconds()
    stop_mask = classified['status'].isin(['STOP', 'PARTIAL_STOP'])
    obj['DurationWithoutError'] = obj['TotalDuration'] - obj['TimeStep'] * len(classified[stop_mask])
    
    if 'DIRECTION_WIND' in estimated_data.columns and 'DIRECTION_NACELLE' in estimated_data.columns:
        obj['YawLag'] = yaw_errors(estimated_data)

    obj['UpPeriodsCount'] = R
    obj['DownPeriodsCount'] = U
    obj['UpPerodsDuration'] = R * resolution.total_seconds()
    obj['DownPerodsDuration'] = U * resolution.total_seconds()

    return obj