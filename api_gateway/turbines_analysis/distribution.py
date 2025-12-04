"""Turbine distribution views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from django.core.cache import cache
from facilities.models import Turbines
from analytics.models import Computation, ClassificationPoint
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.distribution_helpers import (
    calculate_global_distribution,
    calculate_monthly_distribution,
    calculate_day_night_distribution,
    calculate_seasonal_distribution,
    prepare_dataframe_from_classification_points
)


class DistributionAPIView(APIView):
    """
    API để tính phân phối cho các loại dữ liệu của turbine (tốc độ gió, công suất)
    Parameters:
    - turbine_id (bắt buộc): ID của turbine
    - source_type (tùy chọn): Loại dữ liệu (wind_speed, power). Mặc định: wind_speed
    - start_time, end_time (tùy chọn): Thời gian bắt đầu và kết thúc tính phân phối
    - bin_width (tùy chọn): Độ rộng của bin (mặc định: 1 m/s cho wind_speed, 100 kW cho power)
    - bin_count (tùy chọn): Số lượng bin (mặc định: 50)
    - mode (tùy chọn): global (mặc định) hoặc time
    - time_type (tùy chọn, chỉ cần khi mode=time): monthly, day_night, seasonally
    """
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
            
            # Lấy loại dữ liệu (mặc định là wind_speed)
            source_type = request.query_params.get('source_type', 'wind_speed')
            valid_source_types = ['wind_speed', 'power']
            if source_type not in valid_source_types:
                return Response({
                    "success": False,
                    "error": f"source_type must be one of: {', '.join(valid_source_types)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Bin width mặc định dựa vào loại dữ liệu
            default_bin_width = {
                'wind_speed': 1.0,
                'power': 100.0,
            }
            try:
                bin_width = float(request.query_params.get('bin_width', default_bin_width[source_type]))
            except ValueError:
                return Response({
                    "success": False,
                    "error": "bin_width must be a number",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                bin_count = int(request.query_params.get('bin_count', 50))
            except ValueError:
                return Response({
                    "success": False,
                    "error": "bin_count must be an integer",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            mode = request.query_params.get('mode', 'global')
            valid_modes = ['global', 'time']
            if mode not in valid_modes:
                return Response({
                    "success": False,
                    "error": f"mode must be one of: {', '.join(valid_modes)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            time_type = None
            if mode == 'time':
                time_type = request.query_params.get('time_type')
                valid_time_types = ['monthly', 'day_night', 'seasonally']
                
                if not time_type:
                    return Response({
                        "success": False,
                        "error": "time_type must be specified when mode is 'time'",
                        "code": "MISSING_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                if time_type not in valid_time_types:
                    return Response({
                        "success": False,
                        "error": f"time_type must be one of: {', '.join(valid_time_types)}",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy thời gian nếu được cung cấp
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            if start_time:
                try:
                    start_time = int(start_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "start_time must be an integer (Unix timestamp in milliseconds)",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            if end_time:
                try:
                    end_time = int(end_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "end_time must be an integer (Unix timestamp in milliseconds)",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            # Cache key
            cache_key = f"distribution_{turbine_id}_{source_type}_{start_time or 'all'}_{end_time or 'all'}_{bin_width}_{mode}_{time_type or 'none'}_{bin_count}"
            
            # Kiểm tra cache
            cached_result = cache.get(cache_key)
            if cached_result:
                return Response({
                    "success": True,
                    "data": cached_result
                })
            
            # Tính toán phân phối
            result = self._calculate_distribution(
                turbine,
                start_time,
                end_time,
                bin_width,
                bin_count,
                mode,
                time_type,
                source_type
            )
            
            if isinstance(result, Response):
                return result
            
            if not result:
                return Response({
                    "success": False,
                    "error": f"Error calculating {source_type} distribution",
                    "code": "PROCESSING_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            # Lưu vào cache
            cache.set(cache_key, result["data"], timeout=3600)
            
            return Response(result)
        
        except Exception as e:
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _calculate_distribution(
        self, 
        turbine, 
        start_time, 
        end_time, 
        bin_width, 
        bin_count,
        mode, 
        time_type=None, 
        source_type='wind_speed'
    ):
        """Calculate distribution"""
        try:
            # Xây dựng query base với select_related và prefetch_related để tối ưu
            computation_query = Computation.objects.filter(
                turbine=turbine,
                computation_type='classification',
                is_latest=True
            ).select_related('turbine', 'farm')
            
            if start_time and end_time:
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                return Response({
                    "success": False,
                    "error": "No classification computation found for this turbine",
                    "code": "NO_COMPUTATION"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Lấy classification points với filter timestamp nếu có
            classification_points_query = ClassificationPoint.objects.filter(
                computation=computation
            ).only('timestamp', 'wind_speed', 'active_power')
            
            if start_time and end_time:
                classification_points_query = classification_points_query.filter(
                    timestamp__gte=start_time,
                    timestamp__lte=end_time
                )
            
            # Sử dụng iterator để tối ưu memory cho dataset lớn
            classification_points = classification_points_query.order_by('timestamp')
            
            # Chuẩn bị DataFrame từ classification points
            df = prepare_dataframe_from_classification_points(
                classification_points,
                source_type
            )
            
            if df is None or df.empty:
                return Response({
                    "success": False,
                    "error": f"No {source_type} data found for this turbine in specified time range",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Tính toán phân phối dựa trên mode
            result = None
            if mode == 'global':
                result = calculate_global_distribution(df, bin_width, source_type, bin_count)
            elif mode == 'time':
                if time_type == 'monthly':
                    result = calculate_monthly_distribution(df, bin_width, source_type, bin_count)
                elif time_type == 'day_night':
                    result = calculate_day_night_distribution(df, bin_width, source_type, bin_count)
                elif time_type == 'seasonally':
                    result = calculate_seasonal_distribution(df, bin_width, source_type, bin_count)
            
            if result is None:
                return Response({
                    "success": False,
                    "error": "Error calculating distribution",
                    "code": "CALCULATION_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            # Thêm metadata vào kết quả
            result.update({
                'turbine_id': turbine.id,
                'turbine_name': turbine.name,
                'start_time': computation.start_time,
                'end_time': computation.end_time,
                'farm_name': turbine.farm.name if turbine.farm else None,
                'mode': mode,
                'time_type': time_type,
                'source_type': source_type,
            })
            
            return {
                "success": True,
                "data": result
            }
        
        except Exception as e:
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
