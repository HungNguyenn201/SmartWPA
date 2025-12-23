import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated

from facilities.models import Turbines
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.computation_helper import (
    get_turbine_constants,
    validate_time_range,
    save_computation_results,
    format_computation_output,
    load_turbine_data
)
from analytics.computation.smartWPA import get_wpa

logger = logging.getLogger('api_gateway.turbines_analysis')


class ComputationAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def post(self, request, turbine_id):
        try:
            # Lấy turbine và kiểm tra quyền
            try:
                turbine = Turbines.objects.select_related('farm', 'farm__investor').get(id=turbine_id)
            except Turbines.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Turbine not found",
                    "code": "TURBINE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, turbine, "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            # Parse và validate request data
            data = request.data
            try:
                start_time = int(data['start_time'])
                end_time = int(data['end_time'])
            except (KeyError, ValueError, TypeError):
                return Response({
                    "success": False,
                    "error": "start_time and end_time are required integers (Unix timestamp in milliseconds)",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            is_valid, error_msg = validate_time_range(start_time, end_time)
            if not is_valid:
                return Response({
                    "success": False,
                    "error": error_msg,
                    "code": "INVALID_TIME_RANGE"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy constants và data_source
            try:
                constants = get_turbine_constants(turbine, data.get('constants'))
            except ValueError as e:
                return Response({
                    "success": False,
                    "error": str(e),
                    "code": "MISSING_CONSTANTS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            preferred_source = data.get('data_source', 'db')
            if preferred_source not in ('db', 'file'):
                return Response({
                    "success": False,
                    "error": "data_source must be 'db' or 'file'",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Load dữ liệu
            df, data_source_used, error_info = load_turbine_data(
                turbine, start_time, end_time, preferred_source
            )
            
            if df is None or df.empty:
                error_msg = f"No data found for turbine {turbine_id} in time range [{start_time}, {end_time}]"
                if error_info:
                    sources = '; '.join(f"{k.capitalize()}: {v}" for k, v in error_info.items())
                    error_msg += f". Tried: {sources}" if len(error_info) > 1 else f". {sources}"
                
                logger.error(f"Data loading failed for turbine {turbine_id}: {error_msg}")
                return Response({
                    "success": False,
                    "error": error_msg,
                    "code": "NO_DATA_FOUND",
                    "details": {"turbine_id": turbine_id, "start_time": start_time, 
                               "end_time": end_time, "sources_tried": list(error_info.keys()),
                               "errors": error_info}
                }, status=status.HTTP_404_NOT_FOUND)
            
            if len(df) < 6:
                return Response({
                    "success": False,
                    "error": "Insufficient data points. Need at least 6 data points (1 hour minimum)",
                    "code": "INSUFFICIENT_DATA"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Computation
            try:
                computation_result = get_wpa(df, constants)
            except ValueError as e:
                return Response({
                    "success": False,
                    "error": f"Computation failed: {str(e)}",
                    "code": "COMPUTATION_ERROR"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Save results
            try:
                computation = save_computation_results(
                    turbine, turbine.farm, start_time, end_time, computation_result
                )
            except Exception as e:
                logger.error(f"Failed to save computation results: {str(e)}", exc_info=True)
                return Response({
                    "success": False,
                    "error": f"Failed to save computation results: {str(e)}",
                    "code": "SAVE_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            # Format output
            output = format_computation_output(computation_result)
            output.update({
                'computation_id': computation.id,
                'data_source_used': data_source_used,
                'data_points_count': len(df)
            })
            
            return Response({"success": True, "data": output}, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Unexpected error in computation API: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": f"An unexpected error occurred: {str(e)}",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
