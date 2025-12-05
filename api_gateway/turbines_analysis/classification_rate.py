"""Turbine classification rate views"""
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

logger = logging.getLogger('api_gateway.turbines_analysis')


class ClassificationRateAPIView(APIView):
    """API để hiển thị tỷ lệ phân loại cho turbine đã được tính toán trước đó"""
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
            
            # Lấy turbine với select_related để tối ưu query
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
            
            # Lấy tham số từ query
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            # Xây dựng query base với select_related và prefetch_related để tối ưu
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
                    return Response({
                        "success": False,
                        "error": "start_time and end_time must be integers",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                # Tìm computation cho khoảng thời gian cụ thể
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                # Lấy computation mới nhất
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No classification computation found for turbine {turbine_id}")
                return Response({
                    "success": False,
                    "error": "No classification found for this turbine",
                    "code": "NO_CLASSIFICATION"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Lấy dữ liệu classification (đã được prefetch)
            classification_summary = computation.classification_summary.all().order_by('status_code')
            
            # Chuyển đổi thành format JSON
            classification_rates = {}
            classification_map = {}
            for data in classification_summary:
                classification_rates[str(data.status_code)] = data.percentage
                classification_map[str(data.status_code)] = data.status_name
            
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "classification_rates": classification_rates,
                "classification_map": classification_map,
            }
            
            return Response({
                "success": True,
                "data": result
            })
            
        except Exception as e:
            logger.error(f"Error in ClassificationRateAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
