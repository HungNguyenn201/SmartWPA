import logging
from pathlib import Path
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
import pandas as pd
import numpy as np
from facilities.models import Turbines
from analytics.models import Computation, ClassificationPoint
from acquisition.models import FactoryHistorical
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.speed_analysis_helpers import (
    calculate_global_distribution,
    calculate_monthly_distribution,
    calculate_day_night_distribution,
    calculate_seasonal_distribution,
    prepare_dataframe_from_classification_and_historical
)
from api_gateway.turbines_analysis.helpers._header import (
    DEFAULT_DATA_DIR, CSV_SEPARATOR, CSV_ENCODING,
    CSV_DATETIME_FORMAT, CSV_DATETIME_DAYFIRST, FIELD_MAPPING
)

logger = logging.getLogger('api_gateway.turbines_analysis')


class WindSpeedAnalysisAPIView(APIView):
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
            
            permission_response = check_object_permission(
                request, self, turbine,
                "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            try:
                bin_width = float(request.query_params.get('bin_width', 1.0))
                threshold1 = float(request.query_params.get('threshold1', 4.0))
                threshold2 = float(request.query_params.get('threshold2', 8.0))
                sectors_number = int(request.query_params.get('sectors_number', 16))
            except ValueError:
                return Response({
                    "success": False,
                    "error": "Invalid parameter values",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            if sectors_number not in [4, 8, 12, 16, 24, 36]:
                return Response({
                    "success": False,
                    "error": "sectors_number must be one of: 4, 8, 12, 16, 24, 36",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            mode = request.query_params.get('mode', 'global')
            if mode not in ['global', 'time']:
                return Response({
                    "success": False,
                    "error": "mode must be one of: global, time",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            time_type = None
            if mode == 'time':
                time_type = request.query_params.get('time_type')
                if not time_type or time_type not in ['monthly', 'day_night', 'seasonally']:
                    return Response({
                        "success": False,
                        "error": "time_type must be one of: monthly, day_night, seasonally",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            if start_time:
                try:
                    start_time = int(start_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "start_time must be an integer",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            if end_time:
                try:
                    end_time = int(end_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "end_time must be an integer",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            result = self._calculate_wind_distribution(
                turbine, start_time, end_time, bin_width,
                threshold1, threshold2, sectors_number, mode, time_type
            )
            
            if isinstance(result, Response):
                return result
            
            if not result:
                return Response({
                    "success": False,
                    "error": "Error calculating wind distribution",
                    "code": "PROCESSING_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return Response(result)
        
        except Exception as e:
            logger.error(f"Error in WindSpeedAnalysisAPIView.get: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _load_direction_from_file(self, turbine, start_time, end_time):
        try:
            farm_id = turbine.farm.id
            turbine_id = turbine.id
            data_path = Path(DEFAULT_DATA_DIR) / f"Farm{farm_id}" / f"WT{turbine_id}"
            file_path = data_path / "DIRECTION_WIND.csv"
            
            if not file_path.exists():
                return None
            
            start_dt = pd.to_datetime(start_time, unit='ms')
            end_dt = pd.to_datetime(end_time, unit='ms')
            
            df = pd.read_csv(file_path, sep=CSV_SEPARATOR, encoding=CSV_ENCODING)
            
            if df.empty:
                return None
            
            df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'], format=CSV_DATETIME_FORMAT, dayfirst=CSV_DATETIME_DAYFIRST)
            
            direction_column = FIELD_MAPPING.get('DIRECTION_WIND.csv', 'DIRECTION_WIND')
            if direction_column not in df.columns:
                direction_column = df.columns[1]
            
            df = df.rename(columns={'DATE_TIME': 'time_stamp', direction_column: 'wind_dir'})
            df = df[(df['time_stamp'] >= start_dt) & (df['time_stamp'] <= end_dt)]
            
            if df.empty:
                return None
            
            historical_data_list = []
            for _, row in df.iterrows():
                wind_dir = row['wind_dir']
                if pd.notna(wind_dir) and not np.isinf(wind_dir):
                    historical_data_list.append({
                        'time_stamp': row['time_stamp'],
                        'wind_dir': float(wind_dir)
                    })
            
            return historical_data_list if historical_data_list else None
            
        except Exception as e:
            logger.error(f"Error loading direction from file for turbine {turbine.id}: {str(e)}", exc_info=True)
            return None
    
    def _calculate_wind_distribution(
        self, turbine, start_time, end_time, bin_width,
        threshold1, threshold2, sectors_number, mode='global', time_type=None
    ):
        try:
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
            
            if not start_time:
                start_time = computation.start_time
            if not end_time:
                end_time = computation.end_time
            
            if not start_time or not end_time:
                return Response({
                    "success": False,
                    "error": "Invalid time range: start_time and end_time must be provided",
                    "code": "INVALID_TIME_RANGE"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            computation_matches_range = (
                computation.start_time == start_time and 
                computation.end_time == end_time
            )
            
            if computation_matches_range:
                classification_points_query = ClassificationPoint.objects.filter(
                    computation=computation
                ).only('timestamp', 'wind_speed').order_by('timestamp')
            else:
                classification_points_query = ClassificationPoint.objects.filter(
                    computation=computation,
                    timestamp__gte=start_time,
                    timestamp__lte=end_time
                ).only('timestamp', 'wind_speed').order_by('timestamp')
            
            if not classification_points_query.exists():
                return Response({
                    "success": False,
                    "error": "No classification points found for this turbine in specified time range",
                    "code": "NO_CLASSIFICATION_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            historical_data_list = None
            try:
                start_dt = pd.to_datetime(start_time, unit='ms')
                end_dt = pd.to_datetime(end_time, unit='ms')
                
                historical_data = FactoryHistorical.objects.filter(
                    turbine=turbine,
                    time_stamp__gte=start_dt,
                    time_stamp__lte=end_dt,
                    wind_dir__isnull=False
                ).only('time_stamp', 'wind_dir').order_by('time_stamp')
                
                if historical_data.exists():
                    historical_data_list = [
                        {'time_stamp': hist.time_stamp, 'wind_dir': hist.wind_dir}
                        for hist in historical_data.iterator(chunk_size=1000)
                    ]
                else:
                    historical_data_list = self._load_direction_from_file(turbine, start_time, end_time)
            except Exception:
                try:
                    historical_data_list = self._load_direction_from_file(turbine, start_time, end_time)
                except Exception:
                    historical_data_list = None
            
            df = prepare_dataframe_from_classification_and_historical(
                classification_points_query,
                historical_data_list
            )
            
            if df is None or df.empty:
                return Response({
                    "success": False,
                    "error": "No valid wind speed data found for this turbine in specified time range after processing",
                    "code": "NO_VALID_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            distribution_result = None
            if mode == 'global':
                distribution_result = calculate_global_distribution(
                    df, bin_width, threshold1, threshold2, sectors_number
                )
            elif mode == 'time':
                if time_type == 'monthly':
                    distribution_result = calculate_monthly_distribution(
                        df, bin_width, threshold1, threshold2, sectors_number
                    )
                elif time_type == 'day_night':
                    distribution_result = calculate_day_night_distribution(
                        df, bin_width, threshold1, threshold2, sectors_number
                    )
                elif time_type == 'seasonally':
                    distribution_result = calculate_seasonal_distribution(
                        df, bin_width, threshold1, threshold2, sectors_number
                    )
            
            if distribution_result is None:
                logger.error(f"Error calculating wind distribution for turbine {turbine.id}, mode={mode}, time_type={time_type}")
                return Response({
                    "success": False,
                    "error": "Error calculating distribution",
                    "code": "CALCULATION_ERROR"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            distribution_result.update({
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": computation.start_time,
                "end_time": computation.end_time,
                "mode": mode,
                "time_type": time_type
            })
            
            return {
                "success": True,
                "data": distribution_result
            }
        
        except Exception as e:
            logger.error(f"Error in WindSpeedAnalysisAPIView._calculate_wind_distribution for turbine {turbine.id}: {str(e)}", exc_info=True)
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
