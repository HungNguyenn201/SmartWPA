import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from django.core.cache import cache
from facilities.models import Turbines
from analytics.models import Computation, ClassificationPoint
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers.distribution_helpers import (
    calculate_global_distribution,
    calculate_monthly_distribution,
    calculate_day_night_distribution,
    calculate_seasonal_distribution,
    prepare_dataframe_from_classification_points
)

logger = logging.getLogger('api_gateway.turbines_analysis')


class DistributionAPIView(APIView):
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
            
            source_type = request.query_params.get('source_type', 'wind_speed')
            valid_source_types = ['wind_speed', 'power']
            if source_type not in valid_source_types:
                return error_response(f"source_type must be one of: {', '.join(valid_source_types)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            default_bin_width = {
                'wind_speed': 1.0,
                'power': 100.0,
            }
            try:
                bin_width = float(request.query_params.get('bin_width', default_bin_width[source_type]))
            except ValueError:
                return error_response("bin_width must be a number", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            try:
                bin_count = int(request.query_params.get('bin_count', 50))
            except ValueError:
                return error_response("bin_count must be an integer", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            mode = request.query_params.get('mode', 'global')
            valid_modes = ['global', 'time']
            if mode not in valid_modes:
                return error_response(f"mode must be one of: {', '.join(valid_modes)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            time_type = None
            if mode == 'time':
                time_type = request.query_params.get('time_type')
                valid_time_types = ['monthly', 'day_night', 'seasonally']
                
                if not time_type:
                    return error_response("time_type must be specified when mode is 'time'", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                if time_type not in valid_time_types:
                    return error_response(f"time_type must be one of: {', '.join(valid_time_types)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            if start_time:
                try:
                    start_time = int(start_time)
                except ValueError:
                    return error_response("start_time must be an integer (Unix timestamp in milliseconds)", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            if end_time:
                try:
                    end_time = int(end_time)
                except ValueError:
                    return error_response("end_time must be an integer (Unix timestamp in milliseconds)", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            cache_key = f"distribution_{turbine_id}_{source_type}_{start_time or 'all'}_{end_time or 'all'}_{bin_width}_{mode}_{time_type or 'none'}_{bin_count}"
            
            cached_result = cache.get(cache_key)
            if cached_result:
                return success_response(cached_result)
            
            result = self._calculate_distribution(
                turbine,
                start_time,
                end_time,
                bin_width,
                bin_count,
                mode,
                time_type,
                source_type
            )
            
            if isinstance(result, Response):
                return result
            
            if not result:
                return error_response(f"Error calculating {source_type} distribution", "PROCESSING_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            cache.set(cache_key, result["data"], timeout=3600)
            return success_response(result["data"])
        
        except Exception as e:
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _calculate_distribution(
        self, 
        turbine, 
        start_time, 
        end_time, 
        bin_width, 
        bin_count,
        mode, 
        time_type=None, 
        source_type='wind_speed'
    ):
        try:
            computation_query = Computation.objects.filter(
                turbine=turbine,
                computation_type='classification',
                is_latest=True
            ).select_related('turbine', 'farm')
            
            if start_time and end_time:
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No classification computation found for turbine {turbine.id}")
                return error_response("No classification computation found for this turbine", "NO_COMPUTATION", status.HTTP_404_NOT_FOUND)
            
            classification_points_query = ClassificationPoint.objects.filter(
                computation=computation
            ).only('timestamp', 'wind_speed', 'active_power')
            
            if start_time and end_time:
                classification_points_query = classification_points_query.filter(
                    timestamp__gte=start_time,
                    timestamp__lte=end_time
                )
            
            classification_points = classification_points_query.order_by('timestamp')
            
            df = prepare_dataframe_from_classification_points(
                classification_points,
                source_type
            )
            
            if df is None or df.empty:
                logger.warning(f"No {source_type} data found for turbine {turbine.id} in time range {start_time}-{end_time}")
                return error_response(f"No {source_type} data found for this turbine in specified time range", "NO_DATA", status.HTTP_404_NOT_FOUND)
            
            result = None
            if mode == 'global':
                result = calculate_global_distribution(df, bin_width, source_type, bin_count)
            elif mode == 'time':
                if time_type == 'monthly':
                    result = calculate_monthly_distribution(df, bin_width, source_type, bin_count)
                elif time_type == 'day_night':
                    result = calculate_day_night_distribution(df, bin_width, source_type, bin_count)
                elif time_type == 'seasonally':
                    result = calculate_seasonal_distribution(df, bin_width, source_type, bin_count)
            
            if result is None:
                logger.error(f"Error calculating distribution for turbine {turbine.id}, source_type={source_type}, mode={mode}, time_type={time_type}")
                return error_response("Error calculating distribution", "CALCULATION_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            result.update({
                'turbine_id': turbine.id,
                'turbine_name': turbine.name,
                'start_time': computation.start_time,
                'end_time': computation.end_time,
                'farm_name': turbine.farm.name if turbine.farm else None,
                'mode': mode,
                'time_type': time_type,
                'source_type': source_type,
            })
            
            return {
                "success": True,
                "data": result
            }
            
        except Exception as e:
            logger.error(f"Error in DistributionAPIView._calculate_distribution for turbine {turbine.id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
