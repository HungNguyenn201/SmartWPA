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
    prepare_dataframe_from_factory_historical,
    validate_time_range,
    save_computation_results,
    format_computation_output
)
from analytics.computation.smartWPA import get_wpa


class ComputationAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def post(self, request):
        try:
            turbine_id = request.data.get('turbine_id')
            if not turbine_id:
                return Response({
                    "success": False,
                    "error": "turbine_id is required",
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
            
            start_time = request.data.get('start_time')
            end_time = request.data.get('end_time')
            
            if not start_time or not end_time:
                return Response({
                    "success": False,
                    "error": "start_time and end_time are required",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                start_time = int(start_time)
                end_time = int(end_time)
            except (ValueError, TypeError):
                return Response({
                    "success": False,
                    "error": "start_time and end_time must be integers (Unix timestamp in milliseconds)",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            is_valid, error_msg = validate_time_range(start_time, end_time)
            if not is_valid:
                return Response({
                    "success": False,
                    "error": error_msg,
                    "code": "INVALID_TIME_RANGE"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            constants_override = request.data.get('constants')
            try:
                constants = get_turbine_constants(turbine, constants_override)
            except ValueError as e:
                return Response({
                    "success": False,
                    "error": str(e),
                    "code": "MISSING_CONSTANTS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            df = prepare_dataframe_from_factory_historical(turbine, start_time, end_time)
            
            if df is None or df.empty:
                return Response({
                    "success": False,
                    "error": f"No data found for turbine {turbine_id} in time range [{start_time}, {end_time}]",
                    "code": "NO_DATA_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            if len(df) < 6:
                return Response({
                    "success": False,
                    "error": "Insufficient data points. Need at least 6 data points (1 hour minimum)",
                    "code": "INSUFFICIENT_DATA"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                computation_result = get_wpa(df, constants)
            except ValueError as e:
                return Response({
                    "success": False,
                    "error": f"Computation failed: {str(e)}",
                    "code": "COMPUTATION_ERROR"
                }, status=status.HTTP_400_BAD_REQUEST)
            except Exception as e:
                return Response({
                    "success": False,
                    "error": f"Unexpected error during computation: {str(e)}",
                    "code": "INTERNAL_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            try:
                computation = save_computation_results(
                    turbine=turbine,
                    farm=turbine.farm,
                    start_time=start_time,
                    end_time=end_time,
                    computation_result=computation_result
                )
            except Exception as e:
                return Response({
                    "success": False,
                    "error": f"Failed to save computation results: {str(e)}",
                    "code": "SAVE_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            output = format_computation_output(computation_result)
            output['computation_id'] = computation.id
            
            return Response({
                "success": True,
                "data": output
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": f"An unexpected error occurred: {str(e)}",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
