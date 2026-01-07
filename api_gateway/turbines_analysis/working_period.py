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
from api_gateway.turbines_analysis.helpers.working_period_helpers import (
    validate_working_period_params,
    load_working_period_data,
    calculate_performance,
    format_working_period_response,
    get_cache_key
)
from django.core.cache import cache

logger = logging.getLogger('api_gateway.turbines_analysis')


class TurbineWorkingPeriodAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def get(self, request, turbine_id):
        try:
            variation = request.query_params.get('variation', '50')
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            is_valid, error_msg, params = validate_working_period_params(
                variation, start_time, end_time
            )
            
            if not is_valid:
                return Response({
                    "success": False,
                    "error": error_msg,
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
                turbine.id, params['start_time'], params['end_time'],
                params['variation']
            )
            cached_result = cache.get(cache_key)
            if cached_result:
                return Response({
                    "success": True,
                    "data": cached_result
                })
            
            df, data_source_used, error_msg = load_working_period_data(
                turbine, params['start_time'], params['end_time']
            )
            
            if df is None or df.empty:
                return Response({
                    "success": False,
                    "error": error_msg or "No data available for the specified time range",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            performance_data = calculate_performance(
                df, params['variation']
            )
            
            result = format_working_period_response(
                performance_data, turbine, params['start_time'],
                params['end_time'], params['variation']
            )
            
            cache.set(cache_key, result, timeout=3600)
            
            return Response({
                "success": True,
                "data": result
            })
            
        except Exception as e:
            logger.error(f"Error in TurbineWorkingPeriodAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
