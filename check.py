from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework_simplejwt.authentication import JWTAuthentication
import logging
import pandas as pd
import numpy as np
from scipy.stats import weibull_min
from django.core.cache import cache
from API.views.timeseries_manager.timeseries_manager import TimeseriesManager
from management.models import Turbines
from API_simulation.common import load_and_filter_data
from API.views.permissions_views import TurbineFarmPermission

logger = logging.getLogger(__name__)

class WindDistributionView(APIView, TurbineFarmPermission):
    """API để tính toán và trả về dữ liệu phân phối gió và speed rose"""
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        try:
            # Lấy turbine_id từ request
            turbine_id = request.query_params.get('turbine_id')
            
            if not turbine_id:
                return Response({
                    "success": False,
                    "error": "turbine_id must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy các parameters
            bin_width = float(request.query_params.get('bin_width', 1.0))
            threshold1 = float(request.query_params.get('threshold1', 4.0))
            threshold2 = float(request.query_params.get('threshold2', 8.0))
            direction_source = request.query_params.get('direction_source', 'wind_direction')
            sectors_number = int(request.query_params.get('sectors_number', 16))
            
            # Thêm mode và time_type
            mode = request.query_params.get('mode', 'global')
            time_type = request.query_params.get('time_type', None)
            
            # Kiểm tra mode
            valid_modes = ['global', 'time']
            if mode not in valid_modes:
                return Response({
                    "success": False,
                    "error": f"mode must be one of: {', '.join(valid_modes)}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Nếu mode là time, kiểm tra time_type
            if mode == 'time':
                if not time_type:
                    return Response({
                        "success": False,
                        "error": "time_type must be specified when mode is 'time'",
                        "code": "MISSING_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                valid_time_types = ['monthly', 'day_night', 'seasonally']
                if time_type not in valid_time_types:
                    return Response({
                        "success": False,
                        "error": f"time_type must be one of: {', '.join(valid_time_types)}",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy thời gian nếu được cung cấp
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
            # Chuyển đổi thời gian sang integer nếu có
            if start_time:
                try:
                    start_time = int(start_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "start_time must be an integer (Unix timestamp)",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            if end_time:
                try:
                    end_time = int(end_time)
                except ValueError:
                    return Response({
                        "success": False,
                        "error": "end_time must be an integer (Unix timestamp)",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate direction parameters
            if direction_source not in ['wind_direction', 'nacelle_direction']:
                return Response({
                    "success": False,
                    "error": "direction_source must be either 'wind_direction' or 'nacelle_direction'",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            if sectors_number not in [4, 8, 12, 16, 24, 36]:
                return Response({
                    "success": False,
                    "error": "sectors_number must be one of: 4, 8, 12, 16, 24, 36",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy dữ liệu phân phối gió
            result = self._calculate_wind_distribution(
                request.user,
                turbine_id,
                start_time,
                end_time,
                bin_width,
                threshold1,
                threshold2,
                direction_source,
                sectors_number,
                mode,
                time_type
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error in wind distribution calculation: {str(e)}")
            return Response({
                "success": False,
                "error": str(e),
                "code": "PROCESSING_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _convert_timestamp_to_datetime(self, df, timestamp_col='TimeStamp'):

        if timestamp_col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                if np.issubdtype(df[timestamp_col].dtype, np.integer) or np.issubdtype(df[timestamp_col].dtype, np.floating):
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='s')
                else:
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        return df

    def _prepare_bins(self, values, bin_width):
        """Chuẩn bị bins với mật độ cao hơn để đường cong mịn hơn"""
        vmax = np.max(values)
        # Điều chỉnh bin_width để có ít nhất 30 bin và không quá 100 bin
        min_bins = 30
        max_bins = 100
        adjusted_bin_width = bin_width
        bins_count = int(vmax/bin_width) + 2
        
        if bins_count < min_bins:
            adjusted_bin_width = vmax / (min_bins - 2)
        elif bins_count > max_bins:
            adjusted_bin_width = vmax / (max_bins - 2)
        
        bins = np.linspace(0, vmax + adjusted_bin_width, int(vmax/adjusted_bin_width) + 2)
        return bins

    def _compute_histogram(self, values, bins):
        hist, bin_edges = np.histogram(values, bins=bins, density=True)
        hist = hist * 100  # Chuyển sang phần trăm
        return hist, bin_edges

    def _compute_statistics(self, values):
        return np.mean(values), np.max(values), np.min(values)

    def _format_array_values(self, values):
        return [float(val) for val in values]

    def _calculate_wind_distribution(self, user, turbine_id, start_time, end_time, bin_width,
                                  threshold1, threshold2, direction_source, sectors_number, mode='global', time_type=None):
        """Tính toán phân phối gió, năng lượng và speed rose"""
        try:
            # Lấy thông tin turbine
            turbine = Turbines.objects.get(id=turbine_id)
            
            # Kiểm tra quyền truy cập
            if not self._has_access_to_turbine(user, turbine):
                return Response({
                    "success": False,
                    "error": "You don't have permission to access this turbine",
                    "code": "ACCESS_DENIED"
                }, status=status.HTTP_403_FORBIDDEN)
            
            # Kiểm tra cache
            cache_key = f"wind_distribution_{turbine_id}_{start_time}_{end_time}_{bin_width}_{threshold1}_{threshold2}_{direction_source}_{sectors_number}_{mode}_{time_type}"
            cached_result = cache.get(cache_key)
            if cached_result:
                return Response({
                    "success": True,
                    "message": "Using cached result",
                    "data": cached_result
                })
            
            # Lấy dữ liệu
            turbine_identifier, ts_data = self._get_turbine_identifier_and_data(turbine)
            
            if not ts_data:
                return Response({
                    "success": False,
                    "error": f"No timeseries found for turbine {turbine_id}",
                    "code": "NO_TIMESERIES"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Tìm timeseries IDs cho wind_speed và direction
            wind_speed_ts_id, direction_ts_id = self._find_timeseries_ids(ts_data, direction_source)
            
            if not wind_speed_ts_id:
                return Response({
                    "success": False,
                    "error": "No wind speed timeseries found",
                    "code": "NO_TIMESERIES"
                }, status=status.HTTP_404_NOT_FOUND)
            
            if not direction_ts_id:
                return Response({
                    "success": False,
                    "error": f"No {direction_source} timeseries found",
                    "code": "NO_TIMESERIES"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Lấy dữ liệu từ timeseries
            data_frames, _ = load_and_filter_data(
                turbine_identifier,
                [wind_speed_ts_id, direction_ts_id],
                start_time,
                end_time
            )
            
            if not data_frames:
                return Response({
                    "success": False,
                    "error": "No data available for the specified time range",
                    "code": "NO_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Xử lý dữ liệu
            clean_df = self._process_wind_and_direction_data(data_frames, wind_speed_ts_id, direction_ts_id)
            
            if clean_df.empty:
                return Response({
                    "success": False,
                    "error": "No valid data points after filtering",
                    "code": "NO_VALID_DATA"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Tính toán phân phối
            wind_speeds = clean_df['wind_speed'].values
            directions = clean_df['direction'].values
            clean_timestamps = clean_df['timestamp'].values
            
            # Tạo DataFrame sạch
            clean_df = pd.DataFrame({
                'timestamp': clean_timestamps,
                'wind_speed': wind_speeds,
                'direction': directions
            })
            
            
            # Chuyển đổi timestamp sang datetime
            if not pd.api.types.is_datetime64_any_dtype(clean_df['timestamp']):
                if np.issubdtype(clean_df['timestamp'].dtype, np.integer) or np.issubdtype(clean_df['timestamp'].dtype, np.floating):
                    clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'], unit='s')
                else:
                    clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'])
            
            # Tính toán phân phối theo mode
            distribution_result = None
            if mode == 'global':
                distribution_result = self._calculate_global_distribution(
                    clean_df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number
                )
            elif mode == 'time':
                if time_type == 'monthly':
                    distribution_result = self._calculate_monthly_distribution(
                        clean_df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number
                    )
                elif time_type == 'day_night':
                    distribution_result = self._calculate_day_night_distribution(
                        clean_df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number
                    )
                elif time_type == 'seasonally':
                    distribution_result = self._calculate_seasonal_distribution(
                        clean_df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number
                    )
                else:
                    return Response({
                        "success": False,
                        "error": f"Unknown time_type: {time_type}",
                        "code": "INVALID_PARAMETERS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            else:
                return Response({
                    "success": False,
                    "error": f"Unknown mode: {mode}",
                    "code": "INVALID_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Thêm thông tin turbine và farm
            distribution_result.update({
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "custom_id": turbine.custom_id if hasattr(turbine, 'custom_id') else None,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "start_time": start_time,
                "end_time": end_time,
                "mode": mode,
                "time_type": time_type
            })
            
            # Cache kết quả
            cache.set(cache_key, distribution_result, timeout=3600)  # 1 hour
            
            return Response({
                "success": True,
                "data": distribution_result
            })
            
        except Exception as e:
            logger.error(f"Error calculating wind distribution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "error": str(e),
                "code": "PROCESSING_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _get_turbine_identifier_and_data(self, turbine):
        """Lấy turbine identifier và timeseries data"""
        if hasattr(turbine, 'custom_id') and turbine.custom_id:
            turbine_identifier = turbine.custom_id
            ts_data = TimeseriesManager.get_turbine_timeseries_by_custom_id(turbine_identifier)
        else:
            turbine_identifier = str(turbine.id)
            ts_data = TimeseriesManager.get_turbine_timeseries(turbine_identifier)
        
        return turbine_identifier, ts_data

    def _find_timeseries_ids(self, ts_data, direction_source):
        """Tìm timeseries IDs cho wind_speed và direction"""
        wind_speed_ts_id = None
        direction_ts_id = None
        
        for ts_id, field in ts_data.items():
            if field == 'WIND_SPEED':
                wind_speed_ts_id = ts_id
            elif field == 'DIRECTION_WIND' and direction_source == 'wind_direction':
                direction_ts_id = ts_id
            elif field == 'NACELLE_POSITION' and direction_source == 'nacelle_direction':
                direction_ts_id = ts_id
        
        return wind_speed_ts_id, direction_ts_id

    def _process_wind_and_direction_data(self, data_frames, wind_speed_ts_id, direction_ts_id):
        """Xử lý dữ liệu gió và hướng"""
        wind_speed_df = data_frames[wind_speed_ts_id]
        direction_df = data_frames[direction_ts_id]
        
        if wind_speed_df.empty or direction_df.empty:
            return pd.DataFrame()  # Return empty DataFrame
        
        # Chuyển đổi timestamp sang datetime
        for df in [wind_speed_df, direction_df]:
            df = self._convert_timestamp_to_datetime(df)
        
        # Merge wind_speed và direction data
        wind_speed_df = wind_speed_df.set_index('TimeStamp')
        direction_df = direction_df.set_index('TimeStamp')
        combined_df = pd.merge(
            wind_speed_df,
            direction_df,
            left_index=True,
            right_index=True,
            how='inner'
        )
        
        # Reset index để lấy timestamp làm cột
        combined_df = combined_df.reset_index()
        combined_df = combined_df.rename(columns={'index': 'timestamp', 'TimeStamp': 'timestamp'})
        
        # Lấy dữ liệu từ DataFrame
        wind_speeds = combined_df.iloc[:, 1].values  # Wind speed column
        directions = combined_df.iloc[:, 2].values   # Direction column
        
        # Loại bỏ các giá trị NaN và vô hạn
        mask = ~(np.isnan(wind_speeds) | np.isnan(directions) | 
                np.isinf(wind_speeds) | np.isinf(directions))
        
        # Tạo DataFrame sạch
        clean_df = pd.DataFrame({
            'timestamp': combined_df['timestamp'].values[mask],
            'wind_speed': wind_speeds[mask],
            'direction': directions[mask]
        })
        
        return clean_df

    def _calculate_global_distribution(self, df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number):
        """Tính toán phân phối gió và speed rose toàn cục"""
        try:
            # Tính các thông số thống kê
            vmean, vmax, vmin = self._compute_statistics(wind_speeds)

            bins = self._prepare_bins(wind_speeds, bin_width)
            hist, bin_edges = self._compute_histogram(wind_speeds, bins)
            # Tính wind energy distribution (tỷ lệ với v^3)
            wind_energy = wind_speeds ** 3
            energy_hist, _ = np.histogram(wind_speeds, bins=bins, weights=wind_energy, density=True)
            energy_hist = energy_hist * 100  

            shape, loc, scale = weibull_min.fit(wind_speeds, floc=0)
            k = shape  # Weibull shape parameter
            A = scale  # Weibull scale parameter
            
            # Tính đường cong Weibull tại các bin edges
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2  # Sử dụng bin centers
            weibull_curve = (k/A) * (bin_centers/A)**(k-1) * np.exp(-(bin_centers/A)**k) * 100
            
            # Thêm tính toán speed rose
            speed_rose_data = self._calculate_speed_rose(
                wind_speeds,
                directions,
                threshold1,
                threshold2,
                sectors_number
            )
            
            # Tạo dữ liệu phân phối dạng mảng
            bin_values = self._format_array_values(bin_centers)
            wind_distribution_values = self._format_array_values(hist)
            energy_distribution_values = self._format_array_values(energy_hist)
            weibull_values = self._format_array_values(weibull_curve)
            
            result = {
                "statistics": {
                    "vmean": float(vmean),
                    "vmax": float(vmax),
                    "vmin": float(vmin),
                    "weibull_k": float(k),
                    "weibull_A": float(A)
                },
                "distribution_data": {
                    "bin": bin_values,
                    "wind_distribution": wind_distribution_values,
                    "energy_distribution": energy_distribution_values,
                    "weibull_curve": weibull_values
                },
                "speed_rose": speed_rose_data,
                "speed_rose_params": {
                    "threshold1": threshold1,
                    "threshold2": threshold2,
                    "sectors_number": sectors_number
                }
            }
            
            return result
        except Exception as e:
            logger.error(f"Error calculating global distribution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _calculate_monthly_distribution(self, df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number):
        """Tính toán phân phối gió và speed rose theo tháng"""
        try:
            # Thêm cột tháng
            df['month'] = df['timestamp'].dt.month
            
            bins = self._prepare_bins(wind_speeds, bin_width)
            
            # Tên tháng
            month_names = {
                1: 'January', 2: 'February', 3: 'March', 4: 'April',
                5: 'May', 6: 'June', 7: 'July', 8: 'August',
                9: 'September', 10: 'October', 11: 'November', 12: 'December'
            }
            
            # Mảng chứa dữ liệu phân phối cho từng tháng
            months = []
            month_names_array = []
            monthly_data = {}
            monthly_speed_roses = {}
            
            # Khởi tạo mảng để chứa dữ liệu bin cho tất cả tháng
            bin_centers = (bins[:-1] + bins[1:]) / 2
            bin_values = self._format_array_values(bin_centers)
            
            # Khởi tạo dictionary để lưu dữ liệu phân phối cho mỗi tháng
            for month in range(1, 13):
                month_key = str(month)
                monthly_data[month_key] = {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                }
                monthly_speed_roses[month_key] = None
            
            # Xử lý từng tháng
            for month in range(1, 13):
                # Lọc dữ liệu cho tháng hiện tại
                month_df = df[df['month'] == month]
                
                if len(month_df) == 0:
                    continue
                
                months.append(month)
                month_names_array.append(month_names[month])
                
                month_wind_speeds = month_df['wind_speed'].values
                month_directions = month_df['direction'].values
                
                # Tính histogram cho phân phối gió tháng này
                hist, _ = self._compute_histogram(month_wind_speeds, bins)
                
                wind_energy = month_wind_speeds ** 3
                energy_hist, _ = np.histogram(month_wind_speeds, bins=bins, weights=wind_energy, density=True)
                energy_hist = energy_hist * 100  # Chuyển sang phần trăm
                
                # Tính Weibull curve cho tháng này
                shape, loc, scale = weibull_min.fit(month_wind_speeds, floc=0)
                k = shape  # Weibull shape parameter
                A = scale  # Weibull scale parameter
                
                # Tính đường cong Weibull tại các bin centers
                bin_centers = (bins[:-1] + bins[1:]) / 2  # Sử dụng bin centers
                weibull_curve = (k/A) * (bin_centers/A)**(k-1) * np.exp(-(bin_centers/A)**k) * 100
                
                # Tính speed rose cho tháng này
                speed_rose_data = self._calculate_speed_rose(
                    month_wind_speeds,
                    month_directions,
                    threshold1,
                    threshold2,
                    sectors_number
                )
                month_key = str(month)
                monthly_data[month_key]["wind_distribution"] = self._format_array_values(hist)
                monthly_data[month_key]["energy_distribution"] = self._format_array_values(energy_hist)
                monthly_data[month_key]["weibull_curve"] = self._format_array_values(weibull_curve)
                monthly_data[month_key]["weibull_params"] = {
                    "k": float(k),
                    "A": float(A)
                }
                monthly_speed_roses[month_key] = speed_rose_data
            
            # Chỉ giữ lại dữ liệu cho các tháng có dữ liệu
            filtered_monthly_data = {k: v for k, v in monthly_data.items() if k in [str(m) for m in months]}
            filtered_monthly_speed_roses = {k: v for k, v in monthly_speed_roses.items() if k in [str(m) for m in months]}
            
            return {
                "time_mode": "monthly",
                "monthly_distribution": {
                    "months": months,
                    "month_names": month_names_array,
                    "data": {
                        "bin": bin_values,
                        **filtered_monthly_data
                    }
                },
                "monthly_speed_roses": filtered_monthly_speed_roses,
                "speed_rose_params": {
                    "threshold1": threshold1,
                    "threshold2": threshold2,
                    "sectors_number": sectors_number
                },
                "statistics": {
                    "weibull_k": float(k),
                    "weibull_A": float(A)
                }
            }
        
        except Exception as e:
            logger.error(f"Error calculating monthly distribution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _calculate_day_night_distribution(self, df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number):
        """Tính toán phân phối gió và speed rose theo ngày/đêm"""
        try:
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                if np.issubdtype(df['timestamp'].dtype, np.integer) or np.issubdtype(df['timestamp'].dtype, np.floating):
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                else:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Kiểm tra và in ra thông tin
            logger.info(f"Day/Night - Timestamp type: {df['timestamp'].dtype}")
            if not df.empty:
                logger.info(f"Day/Night - First timestamp: {df['timestamp'].iloc[0]}")
                logger.info(f"Day/Night - Last timestamp: {df['timestamp'].iloc[-1]}")
            
            # Thêm cột giờ và phân loại ngày/đêm
            df['hour'] = df['timestamp'].dt.hour
            df['period'] = 'Night'
            df.loc[(df['hour'] >= 6) & (df['hour'] < 18), 'period'] = 'Day'
            # Tạo bins dựa trên bin_width
            bins = self._prepare_bins(wind_speeds, bin_width)
            
            # Dữ liệu bin ở định dạng mảng
            bin_centers = (bins[:-1] + bins[1:]) / 2
            bin_values = self._format_array_values(bin_centers)

            periods = []
            day_night_data = {
                "Day": {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                },
                "Night": {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                }
            }
            day_night_speed_roses = {
                "Day": None,
                "Night": None
            }
            
            # Xử lý từng khoảng thời gian (Day/Night)
            for period in ['Day', 'Night']:
                period_df = df[df['period'] == period]
                
                if len(period_df) == 0:
                    logger.warning(f"No data for period {period}")
                    continue
                
                logger.info(f"Period {period}: {len(period_df)} records")

                periods.append(period)
                period_wind_speeds = period_df['wind_speed'].values
                period_directions = period_df['direction'].values
                
                hist, _ = self._compute_histogram(period_wind_speeds, bins)
                
                wind_energy = period_wind_speeds ** 3
                energy_hist, _ = np.histogram(period_wind_speeds, bins=bins, weights=wind_energy, density=True)
                energy_hist = energy_hist * 100  # Chuyển sang phần trăm

                # Tính Weibull curve cho khoảng thời gian này
                shape, loc, scale = weibull_min.fit(period_wind_speeds, floc=0)
                k = shape  # Weibull shape parameter
                A = scale  # Weibull scale parameter
                
                # Tính đường cong Weibull tại các bin centers
                bin_centers = (bins[:-1] + bins[1:]) / 2
                weibull_curve = (k/A) * (bin_centers/A)**(k-1) * np.exp(-(bin_centers/A)**k) * 100
                
                # Tính speed rose cho tháng này
                speed_rose_data = self._calculate_speed_rose(
                    period_wind_speeds,
                    period_directions,
                    threshold1,
                    threshold2,
                    sectors_number
                )
                
                day_night_data[period]["wind_distribution"] = self._format_array_values(hist)
                day_night_data[period]["energy_distribution"] = self._format_array_values(energy_hist)
                day_night_data[period]["weibull_curve"] = self._format_array_values(weibull_curve)
                day_night_data[period]["weibull_params"] = {
                    "k": float(k),
                    "A": float(A)
                }
                day_night_speed_roses[period] = speed_rose_data
            
            filtered_day_night_data = {k: v for k, v in day_night_data.items() if k in periods}
            filtered_day_night_speed_roses = {k: v for k, v in day_night_speed_roses.items() if k in periods}
            
            return {
                "time_mode": "day_night",
                "day_night_distribution": {
                    "periods": periods,
                    "data": {
                        "bin": bin_values,
                        **filtered_day_night_data
                    }
                },
                "day_night_speed_roses": filtered_day_night_speed_roses,
                "speed_rose_params": {
                    "threshold1": threshold1,
                    "threshold2": threshold2,
                    "sectors_number": sectors_number
                },
                "statistics": {
                    "weibull_k": float(k),
                    "weibull_A": float(A)
                }
            }
        
        except Exception as e:
            logger.error(f"Error calculating day/night distribution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _calculate_seasonal_distribution(self, df, wind_speeds, directions, bin_width, threshold1, threshold2, sectors_number):
        """Tính toán phân phối gió và speed rose theo mùa"""
        try:
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                if np.issubdtype(df['timestamp'].dtype, np.integer) or np.issubdtype(df['timestamp'].dtype, np.floating):
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                else:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
            logger.info(f"Seasonal - Timestamp type: {df['timestamp'].dtype}")
            if not df.empty:
                logger.info(f"Seasonal - First timestamp: {df['timestamp'].iloc[0]}")
                logger.info(f"Seasonal - Last timestamp: {df['timestamp'].iloc[-1]}")

            df['month'] = df['timestamp'].dt.month

            season_map = {
                1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring',
                5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Fall', 10: 'Fall', 11: 'Fall', 12: 'Winter'
            }
            
            df['season'] = df['month'].map(season_map)

            for season in df['season'].unique():
                count = len(df[df['season'] == season])
                logger.info(f"Season {season}: {count} records")

            bins = self._prepare_bins(wind_speeds, bin_width)

            bin_values = self._format_array_values(bins[:-1])
            seasons = []
            seasonal_data = {
                "Winter": {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                },
                "Spring": {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                },
                "Summer": {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                },
                "Fall": {
                    "wind_distribution": [],
                    "energy_distribution": [],
                    "weibull_curve": None,
                    "weibull_params": None
                }
            }
            seasonal_speed_roses = {
                "Winter": None,
                "Spring": None,
                "Summer": None,
                "Fall": None
            }
            
            for season in ['Winter', 'Spring', 'Summer', 'Fall']:
                # Lọc dữ liệu cho mùa hiện tại
                season_df = df[df['season'] == season]
                
                if len(season_df) == 0:
                    logger.warning(f"No data for season {season}")
                    continue
                
                seasons.append(season)
                
                season_wind_speeds = season_df['wind_speed'].values
                season_directions = season_df['direction'].values               
                
                # Tính histogram cho phân phối
                hist, _ = self._compute_histogram(season_wind_speeds, bins)
                
                # Tính wind energy distribution (tỷ lệ với v^3)
                wind_energy = season_wind_speeds ** 3
                energy_hist, _ = np.histogram(season_wind_speeds, bins=bins, weights=wind_energy, density=True)
                energy_hist = energy_hist * 100  # Chuyển sang phần trăm
                
                # Tính Weibull curve cho mùa này
                shape, loc, scale = weibull_min.fit(season_wind_speeds, floc=0)
                k = shape  # Weibull shape parameter
                A = scale  # Weibull scale parameter
                
                # Tính đường cong Weibull tại các bin centers
                bin_centers = (bins[:-1] + bins[1:]) / 2
                weibull_curve = (k/A) * (bin_centers/A)**(k-1) * np.exp(-(bin_centers/A)**k) * 100
                
                # Tính speed rose cho mùa này
                speed_rose_data = self._calculate_speed_rose(
                    season_wind_speeds,
                    season_directions,
                    threshold1,
                    threshold2,
                    sectors_number
                )
                seasonal_data[season]["wind_distribution"] = self._format_array_values(hist)
                seasonal_data[season]["energy_distribution"] = self._format_array_values(energy_hist)
                seasonal_data[season]["weibull_curve"] = self._format_array_values(weibull_curve)
                seasonal_data[season]["weibull_params"] = {
                    "k": float(k),
                    "A": float(A)
                }
                seasonal_speed_roses[season] = speed_rose_data
            
            # Chỉ giữ lại dữ liệu cho các mùa có dữ liệu
            filtered_seasonal_data = {k: v for k, v in seasonal_data.items() if k in seasons}
            filtered_seasonal_speed_roses = {k: v for k, v in seasonal_speed_roses.items() if k in seasons}
            
            return {
                "time_mode": "seasonally",
                "seasonal_distribution": {
                    "data": {
                        "bin": bin_values,
                        **filtered_seasonal_data
                    }
                },
                "seasonal_speed_roses": filtered_seasonal_speed_roses,
                "speed_rose_params": {
                    "threshold1": threshold1,
                    "threshold2": threshold2,
                    "sectors_number": sectors_number
                },
                "statistics": {
                    "weibull_k": float(k),
                    "weibull_A": float(A)
                }
            }

        except Exception as e:
            logger.error(f"Error calculating seasonal distribution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _calculate_speed_rose(self, wind_speeds, directions, threshold1, threshold2, sectors_number):
        """Tính toán speed rose data"""
        try:
            # Tính góc cho mỗi sector
            sector_angle = 360.0 / sectors_number          
            # Khởi tạo mảng để lưu số lượng mẫu cho mỗi sector và mỗi khoảng tốc độ
            sectors = np.zeros((sectors_number, 3))  # 3 khoảng tốc độ: <threshold1, threshold1-threshold2, >threshold2  
            # Phân loại dữ liệu vào các sector và khoảng tốc độ
            for speed, direction in zip(wind_speeds, directions):
                # Tính sector index
                sector_idx = int((direction % 360) / sector_angle)
                
                # Phân loại tốc độ
                if speed < threshold1:
                    speed_category = 0
                elif speed < threshold2:
                    speed_category = 1
                else:
                    speed_category = 2
                
                sectors[sector_idx, speed_category] += 1
            
            total_samples = np.sum(sectors)
            if total_samples > 0:
                sectors = (sectors / total_samples) * 100
            
            angles = [i * sector_angle for i in range(sectors_number)]
            
            return {
                "angle": angles,
                "low_speed": self._format_array_values(sectors[:, 0]),
                "medium_speed": self._format_array_values(sectors[:, 1]),
                "high_speed": self._format_array_values(sectors[:, 2])
            }
            
        except Exception as e:
            logger.error(f"Error calculating speed rose: {str(e)}")
            return {
                "angle": [],
                "low_speed": [],
                "medium_speed": [],
                "high_speed": []
            }
            
        except Exception as e:
            logger.error(f"Error checking turbine access: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _get_data_time_range(self, ts_data):
        """Lấy khoảng thời gian của dữ liệu"""
        try:
            if not ts_data:
                return None, None
            first_ts_id = list(ts_data.keys())[0]
            df = TimeseriesManager.get_turbine_timeseries(first_ts_id)
            
            if df is not None and 'TimeStamp' in df.columns:
                start_time = pd.to_datetime(df['TimeStamp'].min())
                end_time = pd.to_datetime(df['TimeStamp'].max())
                return int(start_time.timestamp()), int(end_time.timestamp())
            return None, None
        except Exception as e:
            logger.error(f"Error getting data time range: {str(e)}")
            return None, None 