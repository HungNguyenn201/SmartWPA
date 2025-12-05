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

logger = logging.getLogger('api_gateway.turbines_analysis')


class TurbinePowerCurveAPIView(APIView):
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
                logger.warning(f"Turbine {turbine_id} not found")
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
            
            mode = request.query_params.get("mode", "global")
            valid_modes = ['global', 'yearly', 'quarterly', 'monthly', 'day/night']
            if mode not in valid_modes:
                return Response({
                    "success": False,
                    "error": f"mode must be one of: {', '.join(valid_modes)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            computation_query = Computation.objects.filter(
                turbine=turbine,
                computation_type='power_curve',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related(
                'power_curve_analyses__power_curve_points'
            )
            
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
                logger.warning(f"No power curve computation found for turbine {turbine_id}")
                return Response({
                    "success": False,
                    "error": "No power curve computation found",
                    "code": "NO_RESULT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            analyses = computation.power_curve_analyses.filter(analysis_mode=mode)
            if not analyses.exists():
                logger.warning(f"No power curve data for mode '{mode}' for turbine {turbine_id}")
                return Response({
                    "success": False,
                    "error": f"No power curve data for mode '{mode}'",
                    "code": "NO_RESULT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            if mode == 'global':
                analysis = analyses.first()
                pts = analysis.power_curve_points.all().order_by('wind_speed')
                power_curve = [
                    {"X": float(p.wind_speed), "Y": float(p.active_power)}
                    for p in pts
                ]
            else:
                power_curve = {}
                for ana in analyses:
                    key = ana.split_value or "unknown"
                    pts = ana.power_curve_points.all().order_by('wind_speed')
                    power_curve[key] = [
                        {"X": float(p.wind_speed), "Y": float(p.active_power)}
                        for p in pts
                    ]
            
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "mode": mode,
                "power_curve": power_curve
            }
            
            return Response({
                "success": True,
                "data": result
            })
            
        except Exception as e:
            logger.error(f"Error in TurbinePowerCurveAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmPowerCurveAPIView(APIView):
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
                logger.warning(f"Farm {farm_id} not found")
                return Response({
                    "success": False,
                    "error": "Farm not found",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
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
                    "error": "No turbines in this farm",
                    "code": "NO_TURBINES"
                }, status=status.HTTP_404_NOT_FOUND)
            
            mode = request.query_params.get("mode", "global")
            valid_modes = ['global', 'yearly', 'quarterly', 'monthly', 'day/night']
            if mode not in valid_modes:
                return Response({
                    "success": False,
                    "error": f"mode must be one of: {', '.join(valid_modes)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            computations = Computation.objects.filter(
                turbine__in=turbines,
                computation_type='power_curve',
                is_latest=True
            ).select_related('turbine', 'farm').prefetch_related(
                'power_curve_analyses__power_curve_points'
            )
            
            turbine_computations = {}
            for comp in computations:
                turbine_id = comp.turbine.id
                if turbine_id not in turbine_computations:
                    turbine_computations[turbine_id] = comp
            
            power_curves = []
            latest_start_time = None
            latest_end_time = None
            
            for turbine in turbines:
                computation = turbine_computations.get(turbine.id)
                if not computation:
                    continue
                
                analyses = computation.power_curve_analyses.filter(analysis_mode=mode)
                if not analyses.exists():
                    continue
                
                if mode == 'global':
                    analysis = analyses.first()
                    pts = analysis.power_curve_points.all().order_by('wind_speed')
                    curve = [
                        {"X": float(p.wind_speed), "Y": float(p.active_power)}
                        for p in pts
                    ]
                else:
                    curve = {}
                    for ana in analyses:
                        key = ana.split_value or "unknown"
                        pts = ana.power_curve_points.all().order_by('wind_speed')
                        curve[key] = [
                            {"X": float(p.wind_speed), "Y": float(p.active_power)}
                            for p in pts
                        ]
                
                if latest_start_time is None or computation.start_time < latest_start_time:
                    latest_start_time = computation.start_time
                if latest_end_time is None or computation.end_time > latest_end_time:
                    latest_end_time = computation.end_time
                
                power_curves.append({
                    "turbine_id": turbine.id,
                    "turbine_name": turbine.name,
                    "power_curve": curve
                })
            
            if not power_curves:
                logger.warning(f"No power curve data for mode '{mode}' in any turbine of farm {farm_id}")
                return Response({
                    "success": False,
                    "error": f"No data for mode '{mode}' in any turbine",
                    "code": "NO_RESULT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": latest_start_time,
                "end_time": latest_end_time,
                "mode": mode,
                "power_curves": power_curves
            }
            
            return Response({
                "success": True,
                "data": result
            })
            
        except Exception as e:
            logger.error(f"Error in FarmPowerCurveAPIView.get for farm {farm_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
