from typing import Dict, Optional, Tuple
import pandas as pd
import numpy as np
from django.db import transaction
from django.utils import timezone

from acquisition.models import FactoryHistorical
from facilities.models import Turbines, Farm
from analytics.models import (
    Computation, PowerCurveAnalysis, PowerCurveData,
    ClassificationSummary, ClassificationPoint,
    IndicatorData, YawErrorData, YawErrorStatistics,
    DailyProduction, CapacityFactorData
)



def get_turbine_constants(turbine: Turbines, constants_override: Optional[Dict] = None) -> Dict:
    if constants_override:
        required = ['V_cutin', 'V_cutout', 'V_rated', 'P_rated', 'Swept_area']
        if all(key in constants_override for key in required):
            return constants_override
    
    raise ValueError(
        "Turbine constants must be provided. Required: V_cutin, V_cutout, V_rated, P_rated, Swept_area"
    )


def prepare_dataframe_from_factory_historical(
    turbine: Turbines,
    start_time: int,
    end_time: int
) -> Optional[pd.DataFrame]:
    start_dt = pd.to_datetime(start_time, unit='ms')
    end_dt = pd.to_datetime(end_time, unit='ms')
    
    historical_data = FactoryHistorical.objects.filter(
        turbine=turbine,
        time_stamp__gte=start_dt,
        time_stamp__lte=end_dt
    ).order_by('time_stamp')
    
    if not historical_data.exists():
        return None
    
    data_list = []
    for hist in historical_data.iterator(chunk_size=1000):
        row = {
            'TIMESTAMP': hist.time_stamp,
            'WIND_SPEED': hist.wind_speed if hist.wind_speed is not None else np.nan,
            'ACTIVE_POWER': hist.active_power if hist.active_power is not None else np.nan,
        }
        
        if hist.wind_dir is not None:
            row['DIRECTION_WIND'] = hist.wind_dir
        
        if hist.air_temp is not None:
            temp = hist.air_temp
            if temp < 223:
                temp = temp + 273.15
            row['TEMPERATURE'] = temp
        
        if hist.pressure is not None:
            row['PRESSURE'] = hist.pressure
        
        if hist.hud is not None:
            row['HUMIDITY'] = hist.hud / 100.0 if hist.hud > 1 else hist.hud
        
        data_list.append(row)
    
    if not data_list:
        return None
    
    df = pd.DataFrame(data_list)
    df = df.sort_values('TIMESTAMP')
    
    return df


def validate_time_range(start_time: int, end_time: int) -> Tuple[bool, Optional[str]]:
    if start_time >= end_time:
        return False, "start_time must be less than end_time"
    
    start_dt = pd.to_datetime(start_time, unit='ms')
    end_dt = pd.to_datetime(end_time, unit='ms')
    
    if (end_dt - start_dt).total_seconds() < 600:
        return False, "Time range must be at least 10 minutes"
    
    return True, None


@transaction.atomic
def save_computation_results(
    turbine: Turbines,
    farm: Farm,
    start_time: int,
    end_time: int,
    computation_result: Dict
) -> Computation:
    computation_type = 'wpa'
    
    result_start_time = computation_result.get('start_time')
    result_end_time = computation_result.get('end_time')
    
    if result_start_time and result_start_time < 1e12:
        result_start_time = int(result_start_time * 1000)
    if result_end_time and result_end_time < 1e12:
        result_end_time = int(result_end_time * 1000)
    
    save_start_time = result_start_time if result_start_time else start_time
    save_end_time = result_end_time if result_end_time else end_time
    
    computation, _ = Computation.objects.update_or_create(
        turbine=turbine,
        farm=farm,
        computation_type=computation_type,
        start_time=save_start_time,
        end_time=save_end_time,
        is_latest=True,
        defaults={
            'created_at': timezone.now()
        }
    )
    
    if 'power_curves' in computation_result:
        save_power_curves(computation, computation_result['power_curves'])
    
    if 'classification' in computation_result:
        save_classification(computation, computation_result['classification'])
    
    indicators = computation_result.get('indicators', {})
    if indicators:
        save_indicators(computation, indicators)
        if 'YawLag' in indicators:
            save_yaw_error(computation, indicators['YawLag'])
    
    return computation


def save_power_curves(computation: Computation, power_curves: Dict):
    for mode, curve_data in power_curves.items():
        if mode == 'global':
            analysis, _ = PowerCurveAnalysis.objects.get_or_create(
                computation=computation,
                analysis_mode=mode,
                defaults={}
            )
            
            PowerCurveData.objects.filter(analysis=analysis).delete()
            
            for wind_speed, active_power in curve_data.items():
                PowerCurveData.objects.create(
                    analysis=analysis,
                    wind_speed=float(wind_speed),
                    active_power=float(active_power)
                )
        else:
            for split_value, curve_data in curve_data.items():
                analysis, _ = PowerCurveAnalysis.objects.get_or_create(
                    computation=computation,
                    analysis_mode=mode,
                    split_value=str(split_value),
                    defaults={}
                )
                
                PowerCurveData.objects.filter(analysis=analysis).delete()
                
                for wind_speed, active_power in curve_data.items():
                    PowerCurveData.objects.create(
                        analysis=analysis,
                        wind_speed=float(wind_speed),
                        active_power=float(active_power)
                    )


def save_classification(computation: Computation, classification: Dict):
    if 'classification_rates' in classification:
        ClassificationSummary.objects.filter(computation=computation).delete()
        
        classification_map = classification.get('classification_map', {})
        classification_rates = classification.get('classification_rates', {})
        
        total_points = sum(classification_rates.values())
        
        for status_code, count in classification_rates.items():
            if count > 0:
                status_name = classification_map.get(status_code, f'Status_{status_code}')
                percentage = (count / total_points * 100) if total_points > 0 else 0.0
                
                ClassificationSummary.objects.create(
                    computation=computation,
                    status_code=int(status_code),
                    status_name=status_name,
                    count=int(count),
                    percentage=float(percentage)
                )
    
    if 'classification_points' in classification:
        ClassificationPoint.objects.filter(computation=computation).delete()
        
        points_data = classification['classification_points']
        if isinstance(points_data, dict) and 'data' in points_data and 'index' in points_data:
            indices = points_data['index']
            data = points_data['data']
            
            wind_speed_idx = None
            active_power_idx = None
            classification_idx = None
            
            if 'columns' in points_data:
                columns = points_data['columns']
                try:
                    wind_speed_idx = columns.index('WIND_SPEED')
                    active_power_idx = columns.index('ACTIVE_POWER')
                    classification_idx = columns.index('classification')
                except ValueError:
                    pass
            
            if wind_speed_idx is not None and active_power_idx is not None and classification_idx is not None:
                points_to_create = []
                for i, idx in enumerate(indices):
                    if i < len(data):
                        row = data[i]
                        if len(row) > max(wind_speed_idx, active_power_idx, classification_idx):
                            try:
                                if isinstance(idx, pd.Timestamp):
                                    timestamp_ms = int(idx.timestamp() * 1000)
                                elif isinstance(idx, (int, float)):
                                    if idx > 1e12:
                                        timestamp_ms = int(idx)
                                    elif idx > 1e9:
                                        timestamp_ms = int(idx * 1000)
                                    else:
                                        timestamp_ms = int(pd.to_datetime(idx).timestamp() * 1000)
                                else:
                                    timestamp_ms = int(pd.to_datetime(idx).timestamp() * 1000)
                                
                                if timestamp_ms:
                                    points_to_create.append(
                                        ClassificationPoint(
                                            computation=computation,
                                            timestamp=timestamp_ms,
                                            wind_speed=float(row[wind_speed_idx]),
                                            active_power=float(row[active_power_idx]),
                                            classification=int(row[classification_idx])
                                        )
                                    )
                            except (ValueError, TypeError, OverflowError):
                                continue
                
                if points_to_create:
                    ClassificationPoint.objects.bulk_create(points_to_create, batch_size=1000)


def save_indicators(computation: Computation, indicators: Dict):
    IndicatorData.objects.filter(computation=computation).delete()
    
    daily_production_list = indicators.pop('DailyProduction', [])
    capacity_factor_dict = indicators.pop('CapacityFactor', {})
    
    indicator_data = IndicatorData(
        computation=computation,
        average_wind_speed=float(indicators.get('AverageWindSpeed', 0.0)),
        reachable_energy=float(indicators.get('ReachableEnergy', 0.0)),
        real_energy=float(indicators.get('RealEnergy', 0.0)),
        loss_energy=float(indicators.get('LossEnergy', 0.0)),
        loss_percent=float(indicators.get('LossPercent', 0.0)),
        rated_power=float(indicators.get('RatedPower', 0.0)),
        tba=float(indicators.get('Tba', 0.0)),
        pba=float(indicators.get('Pba', 0.0)),
        stop_loss=float(indicators.get('StopLoss', 0.0)),
        partial_stop_loss=float(indicators.get('PartialStopLoss', 0.0)),
        under_production_loss=float(indicators.get('UnderProductionLoss', 0.0)),
        curtailment_loss=float(indicators.get('CurtailmentLoss', 0.0)),
        partial_curtailment_loss=float(indicators.get('PartialCurtailmentLoss', 0.0)),
        total_stop_points=int(indicators.get('TotalStopPoints', 0)),
        total_partial_stop_points=int(indicators.get('TotalPartialStopPoints', 0)),
        total_under_production_points=int(indicators.get('TotalUnderProductionPoints', 0)),
        total_curtailment_points=int(indicators.get('TotalCurtailmentPoints', 0)),
        mtbf=float(indicators.get('Mtbf')) if indicators.get('Mtbf') is not None else None,
        mttr=float(indicators.get('Mttr')) if indicators.get('Mttr') is not None else None,
        mttf=float(indicators.get('Mttf')) if indicators.get('Mttf') is not None else None,
        time_step=float(indicators.get('TimeStep', 600.0)),
        total_duration=float(indicators.get('TotalDuration', 0.0)),
        duration_without_error=float(indicators.get('DurationWithoutError', 0.0)),
        up_periods_count=float(indicators.get('UpPeriodsCount', 0.0)),
        down_periods_count=float(indicators.get('DownPeriodsCount', 0.0)),
        up_periods_duration=float(indicators.get('UpPerodsDuration', 0.0)),
        down_periods_duration=float(indicators.get('DownPerodsDuration', 0.0)),
        aep_weibull_turbine=float(indicators.get('AepWeibullTurbine', 0.0)),
        aep_weibull_wind_farm=float(indicators.get('AepWeibullWindFarm')) if indicators.get('AepWeibullWindFarm') is not None else None,
        aep_rayleigh_measured_4=float(indicators.get('AepRayleighMeasured4', 0.0)),
        aep_rayleigh_measured_5=float(indicators.get('AepRayleighMeasured5', 0.0)),
        aep_rayleigh_measured_6=float(indicators.get('AepRayleighMeasured6', 0.0)),
        aep_rayleigh_measured_7=float(indicators.get('AepRayleighMeasured7', 0.0)),
        aep_rayleigh_measured_8=float(indicators.get('AepRayleighMeasured8', 0.0)),
        aep_rayleigh_measured_9=float(indicators.get('AepRayleighMeasured9', 0.0)),
        aep_rayleigh_measured_10=float(indicators.get('AepRayleighMeasured10', 0.0)),
        aep_rayleigh_measured_11=float(indicators.get('AepRayleighMeasured11', 0.0)),
        aep_rayleigh_extrapolated_4=float(indicators.get('AepRayleighExtrapolated4', 0.0)),
        aep_rayleigh_extrapolated_5=float(indicators.get('AepRayleighExtrapolated5', 0.0)),
        aep_rayleigh_extrapolated_6=float(indicators.get('AepRayleighExtrapolated6', 0.0)),
        aep_rayleigh_extrapolated_7=float(indicators.get('AepRayleighExtrapolated7', 0.0)),
        aep_rayleigh_extrapolated_8=float(indicators.get('AepRayleighExtrapolated8', 0.0)),
        aep_rayleigh_extrapolated_9=float(indicators.get('AepRayleighExtrapolated9', 0.0)),
        aep_rayleigh_extrapolated_10=float(indicators.get('AepRayleighExtrapolated10', 0.0)),
        aep_rayleigh_extrapolated_11=float(indicators.get('AepRayleighExtrapolated11', 0.0)),
        yaw_misalignment=float(indicators.get('YawLag', {}).get('statistics', {}).get('mean_error')) if isinstance(indicators.get('YawLag'), dict) else None
    )
    indicator_data.save()
    
    if daily_production_list:
        DailyProduction.objects.filter(computation=computation).delete()
        daily_productions = []
        for dp in daily_production_list:
            if 'date' in dp and 'DailyProduction' in dp:
                try:
                    date = pd.to_datetime(dp['date']).date()
                    daily_productions.append(
                        DailyProduction(
                            computation=computation,
                            date=date,
                            daily_production=float(dp['DailyProduction'])
                        )
                    )
                except (ValueError, KeyError):
                    continue
        
        if daily_productions:
            DailyProduction.objects.bulk_create(daily_productions, batch_size=1000)
    
    if capacity_factor_dict:
        CapacityFactorData.objects.filter(computation=computation).delete()
        capacity_factors = []
        for wind_speed_bin, capacity_factor in capacity_factor_dict.items():
            capacity_factors.append(
                CapacityFactorData(
                    computation=computation,
                    wind_speed_bin=float(wind_speed_bin),
                    capacity_factor=float(capacity_factor)
                )
            )
        
        if capacity_factors:
            CapacityFactorData.objects.bulk_create(capacity_factors, batch_size=1000)


def save_yaw_error(computation: Computation, yaw_lag: Dict):
    if not isinstance(yaw_lag, dict) or 'data' not in yaw_lag:
        return
    
    YawErrorData.objects.filter(computation=computation).delete()
    YawErrorStatistics.objects.filter(computation=computation).delete()
    
    yaw_data = yaw_lag.get('data', {})
    yaw_points = []
    for angle_str, frequency in yaw_data.items():
        try:
            angle = float(angle_str)
            yaw_points.append(
                YawErrorData(
                    computation=computation,
                    angle=angle,
                    frequency=float(frequency)
                )
            )
        except (ValueError, TypeError):
            continue
    
    if yaw_points:
        YawErrorData.objects.bulk_create(yaw_points, batch_size=1000)
    
    statistics = yaw_lag.get('statistics', {})
    if statistics:
        YawErrorStatistics.objects.update_or_create(
            computation=computation,
            defaults={
                'mean_error': float(statistics.get('mean_error', 0.0)),
                'median_error': float(statistics.get('median_error', 0.0)),
                'std_error': float(statistics.get('std_error', 0.0))
            }
        )


def format_computation_output(computation_result: Dict) -> Dict:
    start_time = computation_result.get('start_time')
    end_time = computation_result.get('end_time')
    
    if start_time and start_time < 1e12:
        start_time = int(start_time * 1000)
    if end_time and end_time < 1e12:
        end_time = int(end_time * 1000)
    
    output = {
        'start_time': start_time,
        'end_time': end_time,
        'power_curves': computation_result.get('power_curves', {}),
        'classification': computation_result.get('classification', {}),
        'indicators': computation_result.get('indicators', {})
    }
    
    return output
