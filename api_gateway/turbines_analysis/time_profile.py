import logging
import time
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
from api_gateway.turbines_analysis.helpers.time_profile_helpers import (
    prepare_combined_dataframe_from_sources,
    calculate_profile
)

logger = logging.getLogger('api_gateway.turbines_analysis')


class TimeProfileAPIView(APIView):
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
            
            sources = request.query_params.getlist('sources', [])
            if not sources:
                sources = ['power', 'wind_speed']
            
            valid_sources = ['power', 'wind_speed', 'wind_direction', 'temperature', 'pressure', 'humidity']
            for source in sources:
                if source not in valid_sources:
                    return Response({
                        "success": False,
                        "error": f"sources must be one of: {', '.join(valid_sources)}",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            profile = request.query_params.get('profile', 'hourly').lower()
            valid_profiles = ['hourly', 'daily', 'monthly', 'seasonally']
            if profile not in valid_profiles:
                return Response({
                    "success": False,
                    "error": f"profile must be one of: {', '.join(valid_profiles)}",
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
            
            result = self._get_turbine_time_profile(
                turbine,
                sources,
                start_time,
                end_time,
                profile
            )
            
            if isinstance(result, Response):
                return result
            
            return Response(result)
        
        except Exception as e:
            logger.error(f"Error in TimeProfileAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _get_turbine_time_profile(self, turbine, sources, start_time, end_time, profile):
        try:
            classification_sources = ['power', 'wind_speed']
            historical_sources = ['wind_direction', 'temperature', 'pressure', 'humidity']
            
            has_classification_sources = any(source in sources for source in classification_sources)
            has_historical_sources = any(source in sources for source in historical_sources)
            
            computation = None
            if has_classification_sources:
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
                    if not has_historical_sources:
                        return Response({
                            "success": False,
                            "error": "No classification computation found for this turbine",
                            "code": "NO_COMPUTATION"
                        }, status=status.HTTP_404_NOT_FOUND)
            
            classification_points_dict = {}
            if computation:
                for source in sources:
                    if source in classification_sources:
                        classification_points_query = ClassificationPoint.objects.filter(
                            computation=computation
                        )
                        
                        if source == 'wind_speed':
                            classification_points_query = classification_points_query.only('timestamp', 'wind_speed')
                        elif source == 'power':
                            classification_points_query = classification_points_query.only('timestamp', 'active_power')
                        
                        if start_time and end_time:
                            classification_points_query = classification_points_query.filter(
                                timestamp__gte=start_time,
                                timestamp__lte=end_time
                            )
                        
                        classification_points = classification_points_query.order_by('timestamp')
                        classification_points_dict[source] = classification_points
            
            historical_data_dict = {}
            for source in sources:
                if source in historical_sources:
                    historical_data_query = FactoryHistorical.objects.filter(
                        turbine=turbine
                    )
                    
                    fields_to_get = ['time_stamp']
                    if source == 'wind_direction':
                        fields_to_get.append('wind_dir')
                        historical_data_query = historical_data_query.filter(wind_dir__isnull=False)
                    elif source == 'temperature':
                        fields_to_get.append('air_temp')
                        historical_data_query = historical_data_query.filter(air_temp__isnull=False)
                    elif source == 'pressure':
                        fields_to_get.append('pressure')
                        historical_data_query = historical_data_query.filter(pressure__isnull=False)
                    elif source == 'humidity':
                        fields_to_get.append('hud')
                        historical_data_query = historical_data_query.filter(hud__isnull=False)
                    
                    historical_data_query = historical_data_query.only(*fields_to_get)
                    
                    if start_time and end_time:
                        start_dt = pd.to_datetime(start_time, unit='ms')
                        end_dt = pd.to_datetime(end_time, unit='ms')
                        historical_data_query = historical_data_query.filter(
                            time_stamp__gte=start_dt,
                            time_stamp__lte=end_dt
                        )
                    
                    historical_data = historical_data_query.order_by('time_stamp')
                    historical_data_dict[source] = historical_data
            
            if start_time is None or end_time is None:
                if computation:
                    if start_time is None:
                        start_time = computation.start_time
                    if end_time is None:
                        end_time = computation.end_time
                else:
                    if historical_data_dict:
                        first_hist = next(iter(historical_data_dict.values()))
                        if first_hist.exists():
                            if start_time is None:
                                first_record = first_hist.order_by('time_stamp').first()
                                if first_record:
                                    start_time = int(first_record.time_stamp.timestamp() * 1000)
                            if end_time is None:
                                last_record = first_hist.order_by('-time_stamp').first()
                                if last_record:
                                    end_time = int(last_record.time_stamp.timestamp() * 1000)
                    
                    if end_time is None:
                        end_time = int(time.time() * 1000)
                    if start_time is None:
                        start_time = end_time - (30 * 24 * 60 * 60 * 1000)
                        end_time = int(time.time() * 1000)
                    if start_time is None:
                        start_time = end_time - (30 * 24 * 60 * 60 * 1000)  # 30 ngày trước (milliseconds)
            
            combined_df = prepare_combined_dataframe_from_sources(
                classification_points_dict,
                historical_data_dict,
                sources
            )
            
            if combined_df is None or combined_df.empty:
                logger.warning(f"No data found for turbine {turbine.id} for sources {sources} in time range {start_time}-{end_time}")
                return Response({
                    "success": False,
                    "error": "No data available for the specified time range and sources",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            profile_data = calculate_profile(combined_df, sources, profile)
            
            if not profile_data:
                logger.error(f"Error calculating profile for turbine {turbine.id}, profile={profile}")
                return Response({
                    "success": False,
                    "error": "Error calculating time profile",
                    "code": "CALCULATION_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            result = {
                "success": True,
                "data": {
                    "turbine_id": turbine.id,
                    "turbine_name": turbine.name,
                    "farm_name": turbine.farm.name if turbine.farm else None,
                    "farm_id": turbine.farm.id if turbine.farm else None,
                    "start_time": start_time,
                    "end_time": end_time,
                    "sources": sources,
                    "profile": profile,
                    "data": profile_data
                }
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Error in TimeProfileAPIView._get_turbine_time_profile for turbine {turbine.id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
