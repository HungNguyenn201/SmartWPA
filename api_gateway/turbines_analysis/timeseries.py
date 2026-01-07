import logging
from typing import Optional
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated

from facilities.models import Turbines
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.timeseries_helpers import (
    load_timeseries_data,
    resample_dataframe,
    format_timeseries_response,
    get_cache_key
)
from django.core.cache import cache
import pandas as pd

logger = logging.getLogger('api_gateway.turbines_analysis')


class TurbineTimeseriesAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def get(self, request, turbine_id):
        try:
            sources = request.query_params.getlist('sources', [])
            additional_sources = request.query_params.getlist('additional_sources', [])
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            mode = request.query_params.get('mode', 'raw')
            
            all_sources = sources + additional_sources
            if not all_sources:
                return Response({
                    "success": False,
                    "error": "At least one source must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            valid_modes = ['raw', 'hourly', 'daily', 'monthly', 'seasonally', 'yearly']
            if mode not in valid_modes:
                return Response({
                    "success": False,
                    "error": f"Invalid mode. Must be one of: {', '.join(valid_modes)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
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
            
            cache_key = get_cache_key(
                turbine.id, all_sources, start_time, end_time, mode
            )
            cached_result = cache.get(cache_key)
            if cached_result:
                return Response({
                    "success": True,
                    "data": cached_result
                })
            
            df, data_source_used, error_msg = load_timeseries_data(
                turbine, all_sources, start_time, end_time
            )
            
            if df is None or df.empty:
                return Response({
                    "success": False,
                    "error": error_msg or "No data available for the specified time range",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            df.set_index('timestamp', inplace=True)
            
            if mode != 'raw':
                if not pd.api.types.is_datetime64_any_dtype(df.index):
                    df.index = pd.to_datetime(df.index, unit='s')
                
                df = resample_dataframe(df, mode)
                
                if pd.api.types.is_datetime64_any_dtype(df.index):
                    df.index = df.index.astype('int64') // 10**9
            
            result = format_timeseries_response(
                df, turbine=turbine, start_time=start_time, end_time=end_time,
                mode=mode, data_source_used=data_source_used
            )
            
            cache.set(cache_key, result, timeout=3600)
            
            return Response({
                "success": True,
                "data": result
            })
            
        except Exception as e:
            logger.error(f"Error in TurbineTimeseriesAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
