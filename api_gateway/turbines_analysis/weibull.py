import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Turbines, Farm
from analytics.models import Computation
from permissions.views import CanViewTurbine, CanViewFarm
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response

logger = logging.getLogger('api_gateway.turbines_analysis')


class TurbineWeibullAPIView(APIView):
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
                logger.warning(f"Turbine {turbine_id} not found")
                return error_response("Turbine not found", "TURBINE_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, turbine,
                "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            computation_query = Computation.objects.filter(
                turbine=turbine,
                computation_type='weibull',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related('weibull_data')
            
            if start_time and end_time:
                try:
                    start_time = int(start_time)
                    end_time = int(end_time)
                except ValueError:
                    return error_response("start_time and end_time must be integers", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No weibull computation found for turbine {turbine_id}")
                return error_response("No weibull distributions found for this turbine", "NO_WEIBULL", status.HTTP_404_NOT_FOUND)
            
            weibull_data = computation.weibull_data.first()
            if not weibull_data:
                logger.warning(f"Weibull data not found for computation {computation.id}")
                return error_response("Weibull data not found for this computation", "NO_WEIBULL_DATA", status.HTTP_404_NOT_FOUND)
            
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "data": {
                    "A": weibull_data.scale_parameter_a,
                    "K": weibull_data.shape_parameter_k,
                    "Vmean": weibull_data.mean_wind_speed
                }
            }
            
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in TurbineWeibullAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmWeibullAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewFarm]
    
    def get(self, request, farm_id=None):
        try:
            if not farm_id:
                farm_id = request.query_params.get('farm_id')
            
            if not farm_id:
                return error_response("Farm ID must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            try:
                farm = Farm.objects.select_related('investor').get(id=farm_id)
            except Farm.DoesNotExist:
                logger.warning(f"Farm {farm_id} not found")
                return error_response("Farm not found", "FARM_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, farm,
                "You don't have permission to access this farm"
            )
            if permission_response:
                return permission_response
            
        
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
             
            computation_query = Computation.objects.filter(
                farm=farm,
                computation_type='weibull',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related('weibull_data')
            
            if start_time and end_time:
                try:
                    start_time = int(start_time)
                    end_time = int(end_time)
                except ValueError:
                    return error_response("start_time and end_time must be integers", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No weibull computation found for farm {farm_id}")
                return error_response("No weibull distributions found for this farm", "NO_WEIBULL", status.HTTP_404_NOT_FOUND)
            
            weibull_data = computation.weibull_data.first()
            if not weibull_data:
                logger.warning(f"Weibull data not found for computation {computation.id}")
                return error_response("Weibull data not found for this computation", "NO_WEIBULL_DATA", status.HTTP_404_NOT_FOUND)
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "data": {
                    "A": weibull_data.scale_parameter_a,
                    "K": weibull_data.shape_parameter_k,
                    "Vmean": weibull_data.mean_wind_speed
                }
            }
            
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in FarmWeibullAPIView.get for farm {farm_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
