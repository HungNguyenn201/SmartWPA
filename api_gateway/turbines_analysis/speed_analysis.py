import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
import pandas as pd
from facilities.models import Turbines
from analytics.models import Computation, ClassificationPoint
from acquisition.models import FactoryHistorical
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.speed_analysis_helpers import (
    calculate_global_distribution,
    calculate_monthly_distribution,
    calculate_day_night_distribution,
    calculate_seasonal_distribution,
    prepare_dataframe_from_classification_and_historical
)

logger = logging.getLogger('api_gateway.turbines_analysis')


class WindSpeedAnalysisAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def get(self, request, turbine_id=None):
        try:
            if not turbine_id:
                turbine_id = request.query_params.get('turbine_id')
            
            if not turbine_id:
                return Response({
                    "success": False,
                    "error": "Turbine ID must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                turbine = Turbines.objects.select_related('farm', 'farm__investor').get(id=turbine_id)
            except Turbines.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Turbine not found",
                    "code": "TURBINE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, turbine,
                "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            try:
                bin_width = float(request.query_params.get('bin_width', 1.0))
            except ValueError:
                return Response({
                    "success": False,
                    "error": "bin_width must be a number",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                threshold1 = float(request.query_params.get('threshold1', 4.0))
                threshold2 = float(request.query_params.get('threshold2', 8.0))
            except ValueError:
                return Response({
                    "success": False,
                    "error": "threshold1 and threshold2 must be numbers",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                sectors_number = int(request.query_params.get('sectors_number', 16))
            except ValueError:
                return Response({
                    "success": False,
                    "error": "sectors_number must be an integer",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            if sectors_number not in [4, 8, 12, 16, 24, 36]:
                return Response({
                    "success": False,
                    "error": "sectors_number must be one of: 4, 8, 12, 16, 24, 36",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            mode = request.query_params.get('mode', 'global')
            valid_modes = ['global', 'time']
            if mode not in valid_modes:
                return Response({
                    "success": False,
                    "error": f"mode must be one of: {', '.join(valid_modes)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            time_type = None
            if mode == 'time':
                time_type = request.query_params.get('time_type')
                valid_time_types = ['monthly', 'day_night', 'seasonally']
                
                if not time_type:
                    return Response({
                        "success": False,
                        "error": "time_type must be specified when mode is 'time'",
                        "code": "MISSING_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                if time_type not in valid_time_types:
                    return Response({
                        "success": False,
                        "error": f"time_type must be one of: {', '.join(valid_time_types)}",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            if start_time:
                try:
                    start_time = int(start_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "start_time must be an integer (Unix timestamp in milliseconds)",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            if end_time:
                try:
                    end_time = int(end_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "end_time must be an integer (Unix timestamp in milliseconds)",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            result = self._calculate_wind_distribution(
                turbine,
                start_time,
                end_time,
                bin_width,
                threshold1,
                threshold2,
                sectors_number,
                mode,
                time_type
            )
            
            if isinstance(result, Response):
                return result
            
            if not result:
                return Response({
                    "success": False,
                    "error": "Error calculating wind distribution",
                    "code": "PROCESSING_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return Response(result)
        
        except Exception as e:
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _calculate_wind_distribution(
        self,
        turbine,
        start_time,
        end_time,
        bin_width,
        threshold1,
        threshold2,
        sectors_number,
        mode='global',
        time_type=None
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
                return Response({
                    "success": False,
                    "error": "No classification computation found for this turbine",
                    "code": "NO_COMPUTATION"
                }, status=status.HTTP_404_NOT_FOUND)
            
            classification_points_query = ClassificationPoint.objects.filter(
                computation=computation
            ).only('timestamp', 'wind_speed')
            
            if start_time and end_time:
                classification_points_query = classification_points_query.filter(
                    timestamp__gte=start_time,
                    timestamp__lte=end_time
                )
            
            classification_points = classification_points_query.order_by('timestamp')
            
            historical_data_list = None
            if start_time and end_time:
                start_dt = pd.to_datetime(start_time, unit='ms')
                end_dt = pd.to_datetime(end_time, unit='ms')
                
                historical_data = FactoryHistorical.objects.filter(
                    turbine=turbine,
                    time_stamp__gte=start_dt,
                    time_stamp__lte=end_dt,
                    wind_dir__isnull=False
                ).only('time_stamp', 'wind_dir').order_by('time_stamp')
                
                if historical_data.exists():
                    historical_data_list = []
                    for hist in historical_data.iterator(chunk_size=1000):
                        historical_data_list.append({
                            'time_stamp': hist.time_stamp,
                            'wind_dir': hist.wind_dir
                        })
            
            df = prepare_dataframe_from_classification_and_historical(
                classification_points,
                historical_data_list
            )
            
            if df is None or df.empty:
                logger.warning(f"No wind speed data found for turbine {turbine.id} in time range {start_time}-{end_time}")
                return Response({
                    "success": False,
                    "error": "No wind speed data found for this turbine in specified time range",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            distribution_result = None
            if mode == 'global':
                distribution_result = calculate_global_distribution(
                    df, bin_width, threshold1, threshold2, sectors_number
                )
            elif mode == 'time':
                if time_type == 'monthly':
                    distribution_result = calculate_monthly_distribution(
                        df, bin_width, threshold1, threshold2, sectors_number
                    )
                elif time_type == 'day_night':
                    distribution_result = calculate_day_night_distribution(
                        df, bin_width, threshold1, threshold2, sectors_number
                    )
                elif time_type == 'seasonally':
                    distribution_result = calculate_seasonal_distribution(
                        df, bin_width, threshold1, threshold2, sectors_number
                    )
            
            if distribution_result is None:
                logger.error(f"Error calculating wind distribution for turbine {turbine.id}, mode={mode}, time_type={time_type}")
                return Response({
                    "success": False,
                    "error": "Error calculating distribution",
                    "code": "CALCULATION_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            distribution_result.update({
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "mode": mode,
                "time_type": time_type
            })
            
            return {
                "success": True,
                "data": distribution_result
            }
        
        except Exception as e:
            logger.error(f"Error in WindSpeedAnalysisAPIView._calculate_wind_distribution for turbine {turbine.id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
