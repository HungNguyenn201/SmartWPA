import logging
from typing import Any, Dict, List

from rest_framework.views import APIView
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Turbines, Farm
from analytics.models import ClassificationPoint, Computation
from permissions.views import CanViewTurbine, CanViewFarm
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers._header import CROSS_ANALYSIS_STATUS_BY_CODE
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response

logger = logging.getLogger('api_gateway.turbines_analysis')


def _status_code_to_name(code: int) -> str:
    if 0 <= int(code) < len(CROSS_ANALYSIS_STATUS_BY_CODE):
        return CROSS_ANALYSIS_STATUS_BY_CODE[int(code)]
    return "UNKNOWN"


def _downsample_uniform(rows: List[Dict[str, Any]], max_points: int) -> List[Dict[str, Any]]:
    if max_points <= 0 or len(rows) <= max_points:
        return rows
    step = float(len(rows) - 1) / float(max_points - 1)
    idxs = [int(round(i * step)) for i in range(max_points)]
    idxs = sorted(set(min(len(rows) - 1, max(0, i)) for i in idxs))
    return [rows[i] for i in idxs]


class TurbinePowerCurveAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def get(self, request, turbine_id=None):
        try:
            if not turbine_id:
                turbine_id = request.query_params.get('turbine_id')
            
            if not turbine_id:
                return error_response("Turbine ID must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            try:
                turbine = Turbines.objects.select_related('farm', 'farm__investor').get(id=turbine_id)
            except Turbines.DoesNotExist:
                logger.warning(f"Turbine {turbine_id} not found")
                return error_response("Turbine not found", "TURBINE_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, turbine,
                "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            mode = request.query_params.get("mode", "global")
            valid_modes = ['global', 'time']
            if mode not in valid_modes:
                return error_response(f"mode must be one of: {', '.join(valid_modes)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            time_type = None
            if mode == 'time':
                time_type = request.query_params.get('time_type')
                valid_time_types = ['yearly', 'seasonally', 'monthly', 'day_night']
                
                if not time_type:
                    return error_response("time_type must be specified when mode is 'time'", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                if time_type not in valid_time_types:
                    return error_response(f"time_type must be one of: {', '.join(valid_time_types)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            analysis_mode = 'global' if mode == 'global' else time_type
            if analysis_mode == 'day_night':
                analysis_mode = 'day/night'
            elif analysis_mode == 'seasonally':
                analysis_mode = 'quarterly'
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            max_points_raw = request.query_params.get('max_points')
            max_points = 20000
            if max_points_raw is not None:
                try:
                    max_points = max(1000, min(200_000, int(max_points_raw)))
                except ValueError:
                    pass

            computation_query = Computation.objects.filter(
                turbine=turbine,
                computation_type='power_curve',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related(
                'power_curve_analyses__power_curve_points'
            )
            
            if start_time and end_time:
                try:
                    start_time = int(start_time)
                    end_time = int(end_time)
                except ValueError:
                    return error_response("start_time and end_time must be integers", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No power curve computation found for turbine {turbine_id}")
                return error_response("No power curve computation found", "NO_RESULT_FOUND", status.HTTP_404_NOT_FOUND)
            
            analyses = computation.power_curve_analyses.filter(analysis_mode=analysis_mode)
            if not analyses.exists():
                logger.warning(f"No power curve data for mode '{mode}' time_type '{time_type}' for turbine {turbine_id}")
                return error_response(
                    f"No power curve data for mode '{mode}'" + (f" with time_type '{time_type}'" if time_type else ""),
                    "NO_RESULT_FOUND",
                    status.HTTP_404_NOT_FOUND,
                )
            
            if analysis_mode == 'global':
                analysis = analyses.first()
                pts = analysis.power_curve_points.all().order_by('wind_speed')
                power_curve = [
                    {"X": float(p.wind_speed), "Y": float(p.active_power)}
                    for p in pts
                ]
            else:
                power_curve = {}
                for ana in analyses:
                    key = ana.split_value or "unknown"
                    pts = ana.power_curve_points.all().order_by('wind_speed')
                    power_curve[key] = [
                        {"X": float(p.wind_speed), "Y": float(p.active_power)}
                        for p in pts
                    ]

            # Scatter points from classification (same period as power curve)
            points_data: List[Dict[str, Any]] = []
            cls_q = Computation.objects.filter(
                turbine=turbine,
                computation_type='classification',
                is_latest=True
            )
            if start_time and end_time:
                try:
                    st_ms, et_ms = int(start_time), int(end_time)
                    cls_comp = cls_q.filter(start_time=st_ms, end_time=et_ms).first()
                except ValueError:
                    cls_comp = cls_q.order_by('-end_time').first()
            else:
                cls_comp = cls_q.order_by('-end_time').first()
            if cls_comp:
                cps = ClassificationPoint.objects.filter(computation=cls_comp).only(
                    'timestamp', 'wind_speed', 'active_power', 'classification'
                )
                if start_time and end_time:
                    try:
                        cps = cps.filter(timestamp__gte=int(start_time), timestamp__lte=int(end_time))
                    except ValueError:
                        pass
                rows = list(cps.values_list('timestamp', 'wind_speed', 'active_power', 'classification'))
                raw_points = [
                    {
                        "timestamp_ms": int(ts),
                        "x": float(ws),
                        "y": float(pw),
                        "group": _status_code_to_name(int(cls)),
                    }
                    for ts, ws, pw, cls in rows
                ]
                raw_points.sort(key=lambda p: p["timestamp_ms"])
                points_data = _downsample_uniform(raw_points, max_points)

            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "mode": mode,
                "time_type": time_type,
                "power_curve": power_curve,
                "points": {
                    "group_by": "classification",
                    "max_points": max_points,
                    "data": points_data,
                },
            }
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in TurbinePowerCurveAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmPowerCurveAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewFarm]
    
    def get(self, request, farm_id=None):
        try:
            if not farm_id:
                farm_id = request.query_params.get('farm_id')
            
            if not farm_id:
                return error_response("Farm ID must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            try:
                farm = Farm.objects.select_related('investor').get(id=farm_id)
            except Farm.DoesNotExist:
                logger.warning(f"Farm {farm_id} not found")
                return error_response("Farm not found", "FARM_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, farm,
                "You don't have permission to access this farm"
            )
            if permission_response:
                return permission_response
            
            turbines = Turbines.objects.filter(farm=farm).select_related('farm', 'farm__investor')
            if not turbines.exists():
                return error_response("No turbines in this farm", "NO_TURBINES", status.HTTP_404_NOT_FOUND)
            
            mode = request.query_params.get("mode", "global")
            valid_modes = ['global', 'time']
            if mode not in valid_modes:
                return error_response(f"mode must be one of: {', '.join(valid_modes)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            time_type = None
            if mode == 'time':
                time_type = request.query_params.get('time_type')
                valid_time_types = ['yearly', 'seasonally', 'monthly', 'day_night']
                if not time_type:
                    return error_response("time_type must be specified when mode is 'time'", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                if time_type not in valid_time_types:
                    return error_response(f"time_type must be one of: {', '.join(valid_time_types)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            analysis_mode = 'global' if mode == 'global' else time_type
            if analysis_mode == 'day_night':
                analysis_mode = 'day/night'
            elif analysis_mode == 'seasonally':
                analysis_mode = 'quarterly'
            
            computations = Computation.objects.filter(
                turbine__in=turbines,
                computation_type='power_curve',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related(
                'power_curve_analyses__power_curve_points'
            )
            
            turbine_computations = {}
            for comp in computations:
                turbine_id = comp.turbine.id
                if turbine_id not in turbine_computations:
                    turbine_computations[turbine_id] = comp
            
            power_curves = []
            latest_start_time = None
            latest_end_time = None
            
            for turbine in turbines:
                computation = turbine_computations.get(turbine.id)
                if not computation:
                    continue
                
                analyses = computation.power_curve_analyses.filter(analysis_mode=analysis_mode)
                if not analyses.exists():
                    continue
                
                if analysis_mode == 'global':
                    analysis = analyses.first()
                    pts = analysis.power_curve_points.all().order_by('wind_speed')
                    curve = [
                        {"X": float(p.wind_speed), "Y": float(p.active_power)}
                        for p in pts
                    ]
                else:
                    curve = {}
                    for ana in analyses:
                        key = ana.split_value or "unknown"
                        pts = ana.power_curve_points.all().order_by('wind_speed')
                        curve[key] = [
                            {"X": float(p.wind_speed), "Y": float(p.active_power)}
                            for p in pts
                        ]
                
                if latest_start_time is None or computation.start_time < latest_start_time:
                    latest_start_time = computation.start_time
                if latest_end_time is None or computation.end_time > latest_end_time:
                    latest_end_time = computation.end_time
                
                power_curves.append({
                    "turbine_id": turbine.id,
                    "turbine_name": turbine.name,
                    "power_curve": curve
                })
            
            if not power_curves:
                logger.warning(f"No power curve data for mode '{mode}' time_type '{time_type}' in any turbine of farm {farm_id}")
                return error_response(
                    f"No data for mode '{mode}'" + (f" with time_type '{time_type}'" if time_type else "") + " in any turbine",
                    "NO_RESULT_FOUND",
                    status.HTTP_404_NOT_FOUND,
                )
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": latest_start_time,
                "end_time": latest_end_time,
                "mode": mode,
                "time_type": time_type,
                "power_curves": power_curves
            }
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in FarmPowerCurveAPIView.get for farm {farm_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
