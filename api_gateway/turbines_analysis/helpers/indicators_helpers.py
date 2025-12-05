from typing import Dict, List
from analytics.models import IndicatorData


def serialize_indicator_data(indicator_data: IndicatorData, daily_production_total=None, capacity_factor_avg=None) -> Dict:
    
    return {
        "AverageWindSpeed": indicator_data.average_wind_speed,
        "ReachableEnergy": indicator_data.reachable_energy,
        "RealEnergy": indicator_data.real_energy,
        "LossEnergy": indicator_data.loss_energy,
        "LossPercent": indicator_data.loss_percent,
        "StopLoss": indicator_data.stop_loss,
        "PartialStopLoss": indicator_data.partial_stop_loss,
        "UnderProductionLoss": indicator_data.under_production_loss,
        "CurtailmentLoss": indicator_data.curtailment_loss,
        "PartialCurtailmentLoss": indicator_data.partial_curtailment_loss,
        "TotalStopPoints": indicator_data.total_stop_points,
        "TotalPartialStopPoints": indicator_data.total_partial_stop_points,
        "TotalUnderProductionPoints": indicator_data.total_under_production_points,
        "TotalCurtailmentPoints": indicator_data.total_curtailment_points,
        "DailyProduction": daily_production_total,
        "RatedPower": indicator_data.rated_power,
        "CapacityFactor": capacity_factor_avg,
        "Tba": indicator_data.tba,
        "Pba": indicator_data.pba,
        "Mtbf": indicator_data.mtbf,
        "Mttr": indicator_data.mttr,
        "Mttf": indicator_data.mttf,
        "AepWeibullTurbine": indicator_data.aep_weibull_turbine,
        "AepWeibullWindFarm": indicator_data.aep_weibull_wind_farm,
        "AepRayleighMeasured4": indicator_data.aep_rayleigh_measured_4,
        "AepRayleighMeasured5": indicator_data.aep_rayleigh_measured_5,
        "AepRayleighMeasured6": indicator_data.aep_rayleigh_measured_6,
        "AepRayleighMeasured7": indicator_data.aep_rayleigh_measured_7,
        "AepRayleighMeasured8": indicator_data.aep_rayleigh_measured_8,
        "AepRayleighMeasured9": indicator_data.aep_rayleigh_measured_9,
        "AepRayleighMeasured10": indicator_data.aep_rayleigh_measured_10,
        "AepRayleighMeasured11": indicator_data.aep_rayleigh_measured_11,
        "AepRayleighExtrapolated4": indicator_data.aep_rayleigh_extrapolated_4,
        "AepRayleighExtrapolated5": indicator_data.aep_rayleigh_extrapolated_5,
        "AepRayleighExtrapolated6": indicator_data.aep_rayleigh_extrapolated_6,
        "AepRayleighExtrapolated7": indicator_data.aep_rayleigh_extrapolated_7,
        "AepRayleighExtrapolated8": indicator_data.aep_rayleigh_extrapolated_8,
        "AepRayleighExtrapolated9": indicator_data.aep_rayleigh_extrapolated_9,
        "AepRayleighExtrapolated10": indicator_data.aep_rayleigh_extrapolated_10,
        "AepRayleighExtrapolated11": indicator_data.aep_rayleigh_extrapolated_11,
        "TimeStep": indicator_data.time_step,
        "TotalDuration": indicator_data.total_duration,
        "DurationWithoutError": indicator_data.duration_without_error,
        "YawMisalignment": indicator_data.yaw_misalignment,
        "UpPeriodsCount": indicator_data.up_periods_count,
        "DownPeriodsCount": indicator_data.down_periods_count,
        "UpPeriodsDuration": indicator_data.up_periods_duration,
        "DownPeriodsDuration": indicator_data.down_periods_duration
    }


def aggregate_turbine_indicators(turbine_indicators: List[Dict]) -> Dict:
    if not turbine_indicators:
        return {
            "AverageWindSpeed": 0,
            "ReachableEnergy": 0,
            "RealEnergy": 0,
            "LossEnergy": 0,
            "LossPercent": 0,
            "DailyProduction": 0,
            "RatedPower": 0,
            "CapacityFactor": 0,
            "Tba": 0,
            "Pba": 0
        }
    
    avg_indicators = [
        "AverageWindSpeed",
        "LossPercent",
        "CapacityFactor",
        "Tba",
        "Pba"
    ]
    
    sum_indicators = [
        "RealEnergy",
        "ReachableEnergy",
        "LossEnergy",
        "DailyProduction",
        "RatedPower"
    ]
    
    farm_indicators = {}
    
    for key in avg_indicators:
        values = [ind.get(key, 0) for ind in turbine_indicators if ind.get(key) is not None]
        if values:
            farm_indicators[key] = round(sum(values) / len(values), 2)
        else:
            farm_indicators[key] = 0
    
    for key in sum_indicators:
        values = [ind.get(key, 0) for ind in turbine_indicators if ind.get(key) is not None]
        farm_indicators[key] = round(sum(values), 2)
    
    return farm_indicators

