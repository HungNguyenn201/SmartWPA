import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Turbines
from analytics.models import Computation
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms

logger = logging.getLogger('api_gateway.turbines_analysis')


class ClassificationRateAPIView(APIView):
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
                computation_type='classification',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related('classification_summary')
            
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
                logger.warning(f"No classification computation found for turbine {turbine_id}")
                return error_response("No classification found for this turbine", "NO_CLASSIFICATION", status.HTTP_404_NOT_FOUND)
            
            classification_summary = computation.classification_summary.all().order_by('status_code')
            
            classification_rates = {}
            classification_map = {}
            for data in classification_summary:
                classification_rates[str(data.status_code)] = data.percentage
                classification_map[str(data.status_code)] = data.status_name
            
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "start_time": to_epoch_ms(computation.start_time) if computation.start_time else None,
                "end_time": to_epoch_ms(computation.end_time) if computation.end_time else None,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "classification_rates": classification_rates,
                "classification_map": classification_map,
            }
            
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in ClassificationRateAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
