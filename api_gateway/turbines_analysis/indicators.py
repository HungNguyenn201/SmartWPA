import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Turbines, Farm
from analytics.models import Computation, DailyProduction, CapacityFactorData
from django.db.models import Sum, Avg
from permissions.views import CanViewTurbine, CanViewFarm
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.indicators_helpers import (
    serialize_indicator_data,
    aggregate_turbine_indicators
)
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms

logger = logging.getLogger('api_gateway.turbines_analysis')


class TurbineIndicatorAPIView(APIView):
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
            
            # Kiểm tra quyền truy cập
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
                computation_type='indicators',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related('indicator_data')
            
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
                logger.warning(f"No indicators computation found for turbine {turbine_id}")
                return error_response("No indicators computed yet for this turbine", "NO_INDICATORS", status.HTTP_404_NOT_FOUND)
            
            indicator_data = computation.indicator_data.first()
            if not indicator_data:
                logger.warning(f"Indicator data not found for computation {computation.id} of turbine {turbine_id}")
                return error_response("Indicator data not found for this computation", "NO_INDICATOR_DATA", status.HTTP_404_NOT_FOUND)
            
            daily_production_result = DailyProduction.objects.filter(
                computation=computation
            ).aggregate(total=Sum('daily_production'))
            daily_production_total = daily_production_result.get('total')
            
            capacity_factor_avg = None
            if getattr(indicator_data, "capacity_factor", None) is None:
                capacity_factor_result = CapacityFactorData.objects.filter(
                    computation=computation
                ).aggregate(avg=Avg('capacity_factor'))
                capacity_factor_avg = capacity_factor_result.get('avg')
            
            indicator_dict = serialize_indicator_data(
                indicator_data, 
                daily_production_total=daily_production_total,
                capacity_factor_avg=capacity_factor_avg
            )
            
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "start_time": to_epoch_ms(computation.start_time) if computation.start_time else None,
                "end_time": to_epoch_ms(computation.end_time) if computation.end_time else None,
                "data": indicator_dict
            }
            
            return success_response(result)
        
        except Exception as e:
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmIndicatorAPIView(APIView):
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
                return error_response("Farm not found", "FARM_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            # Kiểm tra quyền truy cập
            permission_response = check_object_permission(
                request, self, farm,
                "You don't have permission to access this farm"
            )
            if permission_response:
                return permission_response
            
            turbines = Turbines.objects.filter(farm=farm).select_related('farm', 'farm__investor')
            if not turbines.exists():
                return error_response("Farm has no turbines", "NO_TURBINES", status.HTTP_404_NOT_FOUND)
            
            turbine_indicators = []
            latest_start_time = None
            latest_end_time = None
            
            computations = Computation.objects.filter(
                turbine__in=turbines,
                computation_type='indicators',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related(
                'indicator_data',
                'daily_productions',
                'capacity_factors'
            )
            
            turbine_computations = {}
            for computation in computations:
                turbine_id = computation.turbine.id
                if turbine_id not in turbine_computations:
                    turbine_computations[turbine_id] = computation
            
            for turbine in turbines:
                computation = turbine_computations.get(turbine.id)
                
                if computation:
                    indicator_data = computation.indicator_data.first()
                    if indicator_data:
                        daily_productions = computation.daily_productions.all()
                        daily_production_total = sum(dp.daily_production for dp in daily_productions) if daily_productions else None
                        
                        capacity_factor_avg = None
                        if getattr(indicator_data, "capacity_factor", None) is None:
                            capacity_factors = computation.capacity_factors.all()
                            if capacity_factors:
                                capacity_factor_avg = sum(cf.capacity_factor for cf in capacity_factors) / len(capacity_factors)
                        
                        indicator_dict = serialize_indicator_data(
                            indicator_data,
                            daily_production_total=daily_production_total,
                            capacity_factor_avg=capacity_factor_avg
                        )
                        
                        turbine_data = {
                            "turbine_id": turbine.id,
                            "turbine_name": turbine.name,
                            "data": indicator_dict
                        }
                        turbine_indicators.append(turbine_data)
                        
                        # Normalize timestamps from DB to milliseconds
                        comp_start_ms = to_epoch_ms(computation.start_time) or computation.start_time
                        comp_end_ms = to_epoch_ms(computation.end_time) or computation.end_time
                        
                        if latest_start_time is None or comp_start_ms < latest_start_time:
                            latest_start_time = comp_start_ms
                        if latest_end_time is None or comp_end_ms > latest_end_time:
                            latest_end_time = comp_end_ms
            
            if not turbine_indicators:
                logger.warning(f"No indicator data found for any turbine in farm {farm_id}")
                return error_response("No indicator data found for any turbine in this farm", "NO_INDICATORS", status.HTTP_404_NOT_FOUND)
            
            turbine_data_list = [t["data"] for t in turbine_indicators]
            farm_indicators = aggregate_turbine_indicators(turbine_data_list)
            
            # Normalize timestamps to milliseconds before returning
            normalized_start_time = to_epoch_ms(latest_start_time) if latest_start_time is not None else None
            normalized_end_time = to_epoch_ms(latest_end_time) if latest_end_time is not None else None
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": normalized_start_time,
                "end_time": normalized_end_time,
                "data": farm_indicators,
            }
            
            return success_response(result)
        
        except Exception as e:
            logger.error(f"Error in FarmIndicatorAPIView.get for farm {farm_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
