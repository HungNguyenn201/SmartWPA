import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Turbines, Farm
from analytics.models import Computation
from permissions.views import CanViewTurbine, CanViewFarm
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms

logger = logging.getLogger('api_gateway.turbines_analysis')


def _build_yaw_statistics(yaw_error_stats):
    """Build statistics dict with mean/median/std and manual aliases (yaw_misalignment, yaw_lag)."""
    if not yaw_error_stats:
        return None
    d = {
        "mean_error": float(yaw_error_stats.mean_error),
        "median_error": float(yaw_error_stats.median_error),
        "std_error": float(yaw_error_stats.std_error),
    }
    d["yaw_misalignment"] = d["mean_error"]  # manual: mean yaw = yaw misalignment
    d["yaw_lag"] = d["std_error"]  # manual: yaw standard deviation = yaw lag
    return d


class TurbineYawErrorAPIView(APIView):
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
                return error_response("Turbine not found", "TURBINE_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, turbine,
                "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            computation_query = Computation.objects.filter(
                turbine=turbine,
                computation_type='yaw_error',
                is_latest=True
            ).select_related('turbine', 'farm', 'yaw_error_statistics').prefetch_related(
                'yaw_error_points'
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
                return error_response("No yaw error analysis found for this turbine", "NO_YAW_ERROR", status.HTTP_404_NOT_FOUND)
            
            yaw_error_data = computation.yaw_error_points.values('angle', 'frequency').order_by('angle')
            yaw_error_stats = computation.yaw_error_statistics
            
            if not yaw_error_data.exists():
                return error_response("No yaw error data found for this computation", "NO_YAW_ERROR_DATA", status.HTTP_404_NOT_FOUND)
            
            if not yaw_error_stats:
                return error_response("No yaw error statistics found for this computation", "NO_YAW_ERROR_STATS", status.HTTP_404_NOT_FOUND)
            
            stats = _build_yaw_statistics(yaw_error_stats)
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": to_epoch_ms(computation.start_time) if computation.start_time else None,
                "end_time": to_epoch_ms(computation.end_time) if computation.end_time else None,
                "data": [
                    {"X": float(point['angle']), "Y": float(point['frequency'])}
                    for point in yaw_error_data
                ],
                "statistics": stats,
            }
            return success_response(result)

        except Exception as e:
            logger.error(f"Error in TurbineYawErrorAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmYawErrorAPIView(APIView):
    """Farm-level yaw error: returns yaw error data for all turbines in the farm (manual 1.3.5.3 d)."""
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

            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            computation_query = Computation.objects.filter(
                turbine__in=turbines,
                computation_type='yaw_error',
                is_latest=True,
            ).select_related('turbine', 'farm', 'yaw_error_statistics').prefetch_related('yaw_error_points')

            if start_time and end_time:
                try:
                    start_time = int(start_time)
                    end_time = int(end_time)
                except ValueError:
                    return error_response("start_time and end_time must be integers", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                computation_query = computation_query.filter(start_time=start_time, end_time=end_time)
            else:
                computation_query = computation_query.order_by('-end_time')

            computations = {c.turbine_id: c for c in computation_query}

            turbines_data = []
            latest_start_ms = None
            latest_end_ms = None
            for turbine in turbines:
                computation = computations.get(turbine.id)
                if not computation:
                    continue
                yaw_points = computation.yaw_error_points.values('angle', 'frequency').order_by('angle')
                yaw_stats = computation.yaw_error_statistics
                if not yaw_points.exists() or not yaw_stats:
                    continue
                stats = _build_yaw_statistics(yaw_stats)
                comp_start_ms = to_epoch_ms(computation.start_time) if computation.start_time else None
                comp_end_ms = to_epoch_ms(computation.end_time) if computation.end_time else None
                if comp_start_ms is not None and (latest_start_ms is None or comp_start_ms < latest_start_ms):
                    latest_start_ms = comp_start_ms
                if comp_end_ms is not None and (latest_end_ms is None or comp_end_ms > latest_end_ms):
                    latest_end_ms = comp_end_ms
                turbines_data.append({
                    "turbine_id": turbine.id,
                    "turbine_name": turbine.name,
                    "start_time": comp_start_ms,
                    "end_time": comp_end_ms,
                    "data": [{"X": float(p['angle']), "Y": float(p['frequency'])} for p in yaw_points],
                    "statistics": stats,
                })

            if not turbines_data:
                return error_response(
                    "No yaw error analysis found for any turbine in this farm",
                    "NO_YAW_ERROR",
                    status.HTTP_404_NOT_FOUND,
                )

            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": latest_start_ms,
                "end_time": latest_end_ms,
                "turbines": turbines_data,
            }
            return success_response(result)

        except Exception as e:
            logger.error(f"Error in FarmYawErrorAPIView.get for farm {farm_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
