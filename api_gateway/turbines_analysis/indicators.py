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

logger = logging.getLogger('api_gateway.turbines_analysis')


class TurbineIndicatorAPIView(APIView):
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
                    return Response({
                        "success": False,
                        "error": "start_time and end_time must be integers",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No indicators computation found for turbine {turbine_id}")
                return Response({
                    "success": False,
                    "error": "No indicators computed yet for this turbine",
                    "code": "NO_INDICATORS"
                }, status=status.HTTP_404_NOT_FOUND)
            
            indicator_data = computation.indicator_data.first()
            if not indicator_data:
                logger.warning(f"Indicator data not found for computation {computation.id} of turbine {turbine_id}")
                return Response({
                    "success": False,
                    "error": "Indicator data not found for this computation",
                    "code": "NO_INDICATOR_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            daily_production_result = DailyProduction.objects.filter(
                computation=computation
            ).aggregate(total=Sum('daily_production'))
            daily_production_total = daily_production_result.get('total')
            
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
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "data": indicator_dict
            }
            
            return Response({
                "success": True,
                "data": result
            })
        
        except Exception as e:
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmIndicatorAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewFarm]
    
    def get(self, request, farm_id=None):
        try:
            if not farm_id:
                farm_id = request.query_params.get('farm_id')
            
            if not farm_id:
                return Response({
                    "success": False,
                    "error": "Farm ID must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                farm = Farm.objects.select_related('investor').get(id=farm_id)
            except Farm.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Farm not found",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Kiểm tra quyền truy cập
            permission_response = check_object_permission(
                request, self, farm,
                "You don't have permission to access this farm"
            )
            if permission_response:
                return permission_response
            
            turbines = Turbines.objects.filter(farm=farm).select_related('farm', 'farm__investor')
            if not turbines.exists():
                return Response({
                    "success": False,
                    "error": "Farm has no turbines",
                    "code": "NO_TURBINES"
                }, status=status.HTTP_404_NOT_FOUND)
            
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
                        
                        capacity_factors = computation.capacity_factors.all()
                        if capacity_factors:
                            capacity_factor_avg = sum(cf.capacity_factor for cf in capacity_factors) / len(capacity_factors)
                        else:
                            capacity_factor_avg = None
                        
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
                        
                        if latest_start_time is None or computation.start_time < latest_start_time:
                            latest_start_time = computation.start_time
                        if latest_end_time is None or computation.end_time > latest_end_time:
                            latest_end_time = computation.end_time
            
            if not turbine_indicators:
                logger.warning(f"No indicator data found for any turbine in farm {farm_id}")
                return Response({
                    "success": False,
                    "error": "No indicator data found for any turbine in this farm",
                    "code": "NO_INDICATORS"
                }, status=status.HTTP_404_NOT_FOUND)
            
            turbine_data_list = [t["data"] for t in turbine_indicators]
            farm_indicators = aggregate_turbine_indicators(turbine_data_list)
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": latest_start_time,
                "end_time": latest_end_time,
                "data": farm_indicators,
            }
            
            return Response({
                "success": True,
                "data": result
            })
        
        except Exception as e:
            logger.error(f"Error in FarmIndicatorAPIView.get for farm {farm_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
