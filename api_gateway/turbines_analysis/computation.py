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
from analytics.computation.normalize import preprocess_for_constants
from analytics.computation.constants_estimation import derive_turbine_constants_from_scada, ConstantEstimationConfig

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
            constants_override = data.get('constants') or {}
            try:
                # Only non-derivable constants (e.g., Swept_area)
                base_constants = get_turbine_constants(turbine, constants_override)
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

        
            df_for_constants = preprocess_for_constants(df.copy())
            

            cfg = ConstantEstimationConfig()
            constants, _ = derive_turbine_constants_from_scada(
                df_for_constants,
                base_constants=base_constants,
                cfg=cfg,
                include_debug=False,
            )
            
            # Computation
            try:
                computation_result = get_wpa(df, constants)
            except ValueError as e:
                # Important: log server-side as well (previously only returned to client)
                logger.error(
                    f"Computation failed for turbine={turbine_id}, range=[{start_time},{end_time}], "
                    f"data_source={preferred_source}, constants_used={constants}. Error: {str(e)}",
                    exc_info=True,
                )
                return Response({
                    "success": False,
                    "error": f"Computation failed: {str(e)}",
                    "code": "COMPUTATION_ERROR"
                }, status=status.HTTP_400_BAD_REQUEST)
            try:
                saved_computations = save_computation_results(
                    turbine, turbine.farm, start_time, end_time, computation_result, constants=constants
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
            output['data_source_used'] = data_source_used
            output['data_points_count'] = len(df)
            output['constants_used'] = constants
            
            # Thêm computation IDs cho từng type
            output['computation_ids'] = {
                comp_type: comp.id 
                for comp_type, comp in saved_computations.items()
            }
            
            # Log thành công
            computation_types = ', '.join(saved_computations.keys())
            logger.warning(
                f"Computation completed successfully for turbine {turbine_id}. "
                f"Types saved: {computation_types}. "
                f"Data source: {data_source_used}, Data points: {len(df)}"
            )
            
            # Tạo message cho frontend
            computation_count = len(saved_computations)
            message = f"Computation completed successfully. {computation_count} computation type(s) saved: {computation_types}"
            
            return Response({
                "success": True,
                "message": message,
                "data": output
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Unexpected error in computation API: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": f"An unexpected error occurred: {str(e)}",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
