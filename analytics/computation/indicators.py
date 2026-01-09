import pandas as pd
from .estimate import estimate
from .yaw_error import yaw_errors
from .reliability import compute_mttr_mttf_mtbf

def indicators(classified: pd.DataFrame, constants: dict) -> dict:
    obj = {}
    estimated_data = estimate(classified, fill_flag=True)
    resolution = estimated_data.iloc[1].name - estimated_data.iloc[0].name

    obj['AverageWindSpeed'] = estimated_data['WIND_SPEED'].mean()
    obj['ReachableEnergy'] = estimated_data['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['RealEnergy'] = estimated_data['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))

    obj['LossEnergy'] = max(0.0, obj['ReachableEnergy'] - obj['RealEnergy'])
    
    
    if obj['ReachableEnergy'] > 0:
        obj['LossPercent'] = obj['LossEnergy'] / obj['ReachableEnergy']
    else:
        obj['LossPercent'] = 0.0
    
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
    estimated_stop = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    real_stop = tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['StopLoss'] = max(0.0, estimated_stop - real_stop)

    tmp = estimated_data[estimated_data['status'] == 'PARTIAL_STOP']
    estimated_partial_stop = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    real_partial_stop = tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['PartialStopLoss'] = max(0.0, estimated_partial_stop - real_partial_stop)

    tmp = estimated_data[estimated_data['status'] == 'UNDERPRODUCTION']
    estimated_under = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    real_under = tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['UnderProductionLoss'] = max(0.0, estimated_under - real_under)

    tmp = estimated_data[estimated_data['status'] == 'CURTAILMENT']
    estimated_curtail = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    real_curtail = tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['CurtailmentLoss'] = max(0.0, estimated_curtail - real_curtail)

    tmp = estimated_data[estimated_data['status'] == 'PARTIAL_CURTAILMENT']
    estimated_partial_curtail = tmp['ESTIMATED_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    real_partial_curtail = tmp['ACTIVE_POWER'].sum() * (resolution / pd.Timedelta(hours=1))
    obj['PartialCurtailmentLoss'] = max(0.0, estimated_partial_curtail - real_partial_curtail)

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

    # ---------------------------------------------------------------------
    # Reliability KPIs (IEC TS 61400-26-4 inspired, strict mode)
    #
    # Mapping (as agreed):
    # - UP (operating/fit): NORMAL + OVERPRODUCTION
    # - DOWN (failure/repair): STOP
    # - Ignored/degraded: PARTIAL_STOP, CURTAILMENT, PARTIAL_CURTAILMENT,
    #                    UNDERPRODUCTION, MEASUREMENT_ERROR, UNKNOWN
    #
    # Formulas (SCADA time-series, IEC-style):
    #   FailureCount = #(UP -> STOP transitions), with consecutive STOP merged
    #   MTTR = TotalDownTime / FailureCount
    #   MTTF = TotalUpTime / FailureCount
    #   MTBF = MTTF + MTTR
    # ---------------------------------------------------------------------
    rel = compute_mttr_mttf_mtbf(
        classified,
        up_statuses=["NORMAL", "OVERPRODUCTION"],
        down_statuses=["STOP"],
        ignore_statuses=[
            "PARTIAL_STOP",
            "CURTAILMENT",
            "PARTIAL_CURTAILMENT",
            "UNDERPRODUCTION",
            "MEASUREMENT_ERROR",
            "UNKNOWN",
        ],
    )
    obj["Mttr"] = rel.get("Mttr")
    obj["Mttf"] = rel.get("Mttf")
    obj["Mtbf"] = rel.get("Mtbf")
    obj["FailureCount"] = rel.get("FailureCount", 0)

    return obj