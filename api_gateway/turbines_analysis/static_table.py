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
from api_gateway.turbines_analysis.helpers.static_table_helpers import (
    calculate_statistics_from_dataframe,
    prepare_dataframe_from_classification_points,
    prepare_dataframe_from_historical
)

logger = logging.getLogger('api_gateway.turbines_analysis')


class StaticTableAPIView(APIView):
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
            
            sources = request.query_params.getlist('source', [])
            if not sources:
                sources = ['power', 'wind_speed']
            
            valid_source_types = ['wind_speed', 'power', 'wind_direction']
            for source_type in sources:
                if source_type not in valid_source_types:
                    return Response({
                        "success": False,
                        "error": f"source must be one of: {', '.join(valid_source_types)}",
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
            
            all_results = {}
            for source_type in sources:
                result = self._calculate_statistics(
                    turbine,
                    start_time,
                    end_time,
                    source_type
                )
                
                if isinstance(result, Response):
                    return result
                
                if result and result.get('data'):
                    all_results[source_type] = result['data']
            
            if not all_results:
                return Response({
                    "success": False,
                    "error": "Error calculating statistics for all sources",
                    "code": "PROCESSING_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return Response({
                "success": True,
                "data": all_results
            })
        
        except Exception as e:
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _calculate_statistics(self, turbine, start_time, end_time, source_type='wind_speed'):
        try:
            use_classification = source_type in ['wind_speed', 'power']
            use_historical = source_type == 'wind_direction'
            
            df = None
            
            if use_classification:
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
                    logger.warning(f"No classification computation found for turbine {turbine.id} for source_type {source_type}")
                    return Response({
                        "success": False,
                        "error": f"No classification computation found for this turbine",
                        "code": "NO_COMPUTATION"
                    }, status=status.HTTP_404_NOT_FOUND)
                
                classification_points_query = ClassificationPoint.objects.filter(
                    computation=computation
                )
                
                if source_type == 'wind_speed':
                    classification_points_query = classification_points_query.only('timestamp', 'wind_speed')
                elif source_type == 'power':
                    classification_points_query = classification_points_query.only('timestamp', 'active_power')
                
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
            
            elif use_historical:
                historical_data_query = FactoryHistorical.objects.filter(
                    turbine=turbine,
                    wind_dir__isnull=False
                ).only('time_stamp', 'wind_dir')
                
                if start_time and end_time:
                    start_dt = pd.to_datetime(start_time, unit='ms')
                    end_dt = pd.to_datetime(end_time, unit='ms')
                    historical_data_query = historical_data_query.filter(
                        time_stamp__gte=start_dt,
                        time_stamp__lte=end_dt
                    )
                
                historical_data = historical_data_query.order_by('time_stamp')
                
                df = prepare_dataframe_from_historical(
                    historical_data,
                    source_type
                )
            
            if df is None or df.empty:
                logger.warning(f"No {source_type} data found for turbine {turbine.id} in time range {start_time}-{end_time}")
                return Response({
                    "success": False,
                    "error": f"No {source_type} data found for this turbine in specified time range",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            result_data = calculate_statistics_from_dataframe(
                df,
                'value',
                source_type
            )
            
            if not result_data:
                logger.error(f"Error calculating statistics for turbine {turbine.id}, source_type={source_type}")
                return Response({
                    "success": False,
                    "error": f"Error calculating statistics for {source_type}",
                    "code": "CALCULATION_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return {
                "success": True,
                "data": result_data
            }
        
        except Exception as e:
            logger.error(f"Error in StaticTableAPIView._calculate_statistics for turbine {turbine.id}, source_type={source_type}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
