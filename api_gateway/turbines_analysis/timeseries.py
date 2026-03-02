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
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
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
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            mode = request.query_params.get('mode', 'raw')
            
            if not sources:
                return error_response("At least one source must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            valid_modes = ['raw', 'hourly', 'daily', 'monthly', 'seasonally', 'yearly']
            if mode not in valid_modes:
                return error_response(f"Invalid mode. Must be one of: {', '.join(valid_modes)}", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            parsed_start_time = None
            parsed_end_time = None
            
            if start_time:
                try:
                    parsed_start_time = int(start_time)
                except ValueError:
                    return error_response("start_time must be an integer (Unix timestamp in milliseconds)", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            if end_time:
                try:
                    parsed_end_time = int(end_time)
                except ValueError:
                    return error_response("end_time must be an integer (Unix timestamp in milliseconds)", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
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
            
            cache_key = get_cache_key(
                turbine.id, sources, parsed_start_time, parsed_end_time, mode
            )
            cached_result = cache.get(cache_key)
            if cached_result:
                return success_response(cached_result)
            
            df, data_source_used, units_meta, error_msg = load_timeseries_data(
                turbine, sources, parsed_start_time, parsed_end_time
            )
            
            if df is None or df.empty:
                return error_response(
                    error_msg or "No data available for the specified time range",
                    "NO_DATA",
                    status.HTTP_404_NOT_FOUND,
                )
            
            df.set_index('timestamp', inplace=True)
            
            if mode != 'raw':
                if not pd.api.types.is_datetime64_any_dtype(df.index):
                    ms = df.index.to_series().apply(
                        lambda x: to_epoch_ms(x) if pd.notna(x) else None
                    )
                    valid = ms.notna()
                    if valid.any():
                        df = df.loc[valid].copy()
                        df.index = pd.to_datetime(
                            ms[valid].astype("int64"), unit="ms"
                        )
                df = resample_dataframe(df, mode)
                
                if pd.api.types.is_datetime64_any_dtype(df.index):
                    # Convert từ nanoseconds (pandas datetime) sang milliseconds
                    df.index = df.index.astype('int64') // 10**6
            
            result = format_timeseries_response(
                df, turbine=turbine, start_time=parsed_start_time, end_time=parsed_end_time,
                mode=mode, data_source_used=data_source_used, units_meta=units_meta
            )
            
            cache.set(cache_key, result, timeout=3600)
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in TurbineTimeseriesAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
