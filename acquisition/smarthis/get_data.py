import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from .restful_client import *
from .handle_time import *
from ._header import *
from acquisition.models import HISPoint, SmartHIS
from django.core.exceptions import ObjectDoesNotExist
import logging

logger = logging.getLogger(__name__)



def get_data_smartHis(id_farm, target, time_range):
    try:
        interval = '15m'
        if target == "factory":
            df_history = query_data(id_farm, target, time_range, interval, mode="sampled")
        elif target == "turbines":
            df_history = query_data(id_farm, target, time_range, interval, mode="sampled")
        else:
            logger.error(f"Invalid target '{target}' for farm {id_farm}")
            raise ValueError(f"Invalid target: {target}")
        
        if df_history.empty:
            logger.warning(f"No data returned for farm {id_farm}, target {target}")
        
        return df_history
    except Exception as e:
        logger.error(f"Failed to get data for farm {id_farm}, target {target}: {e}", exc_info=True)
        raise


def query_data(id_farm, target, time_range, interval, mode):
    try:
        smart_his = SmartHIS.objects.filter(farm_id=id_farm).first()
        if not smart_his:
            logger.error(f"SmartHIS not found for farm {id_farm}")
            raise ObjectDoesNotExist(f"SmartHIS not found for farm {id_farm}")
        
        address = smart_his.address
        start_time, end_time = (from_str_time_to_milisecond(str(t), day_first=False) for t in time_range)
        
        if start_time >= end_time:
            logger.warning(f"Invalid time range for farm {id_farm}: start_time >= end_time")
            return pd.DataFrame()
        
        points_col = get_points_list(id_farm, target)
        if not points_col:
            logger.warning(f"No active points found for farm {id_farm}, target {target}")
            return pd.DataFrame()
        
        logger.warning(f"Querying {len(points_col)} points for farm {id_farm}, target {target}, mode {mode}")
        
        payload = {"interval": interval}
        if mode == "sampled":
            payload.update({
                "expression_mode": False,
                "retrieval_mode": 2,
            })
        elif mode == "calculated":
            payload.update({
                'calculate_mode': 4,
                'query_mode': 0,
                'query_sample': '1m'
            })
        else:
            logger.error(f"Invalid mode '{mode}' for farm {id_farm}")
            raise ValueError(f"Invalid mode: {mode}")
        
        prev_time = start_time
        points_df = pd.DataFrame()
        chunk_count = 0
        
        while prev_time < end_time:
            token = check_and_get_token(id_farm)
            if token is None:
                logger.error(f"Failed to get token for farm {id_farm} at time {prev_time}")
                break
            
            next_time = min(prev_time + MAXIMUM_LEN_GET_DATA, end_time)
            payload.update({"start_time": prev_time, "end_time": next_time})
            
            try:
                sub_points_df = get_points_df(address, token, payload, points_col, mode)
                
                if not sub_points_df.empty:
                    sub_points_df['TimeStamp'] = make_time_ind(
                        prev_time, next_time, len(sub_points_df), interval)
                    sub_points_df.set_index('TimeStamp', inplace=True)
                    points_df = pd.concat([points_df, sub_points_df], ignore_index=False)
                    chunk_count += 1
                else:
                    logger.warning(f"No data returned for farm {id_farm} in time range {prev_time}-{next_time}")
            except Exception as e:
                logger.error(f"Failed to get points data for farm {id_farm} at time {prev_time}-{next_time}: {e}", exc_info=True)
            
            prev_time = next_time
        
        logger.warning(f"Completed query for farm {id_farm}: {chunk_count} chunks, {len(points_df)} total records")
        return points_df
        
    except Exception as e:
        logger.error(f"Query data failed for farm {id_farm}, target {target}: {e}", exc_info=True)
        raise
def get_points_collection(id_farm, target=None):
    try:
        query = HISPoint.objects.filter(
            farm_id=id_farm,
            is_active=True
        ).select_related('point_type', 'turbine')
        
        if target == 'factory' or target == 'farm':
            query = query.filter(point_type__level='farm', turbine__isnull=True)
        elif target == 'turbines':
            query = query.filter(point_type__level='turbine', turbine__isnull=False)
        
        points_mapping = {}
        for his_point in query:
            column_name = his_point.point_type.column_name
            if his_point.turbine:
                column_name = f"{column_name}_{his_point.turbine.name}"
            points_mapping[his_point.point_name] = column_name
        
        if not points_mapping:
            logger.warning(f"No active points found for farm {id_farm}, target {target}")
        
        return points_mapping
    except Exception as e:
        logger.error(f"Failed to get points collection for farm {id_farm}, target {target}: {e}", exc_info=True)
        raise


def get_points_list(id_farm, target=None):
    try:
        points_mapping = get_points_collection(id_farm, target)
        return list(points_mapping.keys())
    except Exception as e:
        logger.error(f"Failed to get points list for farm {id_farm}, target {target}: {e}", exc_info=True)
        return []


def _fetch_single_point(address, token, payload, point, fetch_function):
    conn = None
    try:
        if not point:
            return None, None
        
        conn = get_connection(address)
        if conn is None:
            logger.error(f"Failed to establish connection for point '{point}'")
            return point, None
        
        point_payload = payload.copy()
        point_payload['point_name'] = point
        response = fetch_function(conn, token, point_payload)
        data = handle_res_sample(response)
        return point, data
    except Exception as e:
        logger.error(f"Failed to fetch point '{point}': {e}", exc_info=True)
        return point, None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_points_df(address, token, arg_point, points_list, mode):
    if not points_list:
        logger.warning("Empty points list provided")
        return pd.DataFrame()
    
    try:
        if mode == "calculated":
            fetch_function = get_calculated
        elif mode == "sampled":
            fetch_function = get_sampled
        else:
            logger.error(f"Unknown mode: {mode}")
            raise ValueError(f"Unknown mode: {mode}")
        
        data_points = {}
        total_points = len(points_list)
        
        if total_points <= BATCH_SIZE:
            for point in points_list:
                point_name, data = _fetch_single_point(address, token, arg_point, point, fetch_function)
                if point_name and data is not None:
                    data_points[point_name] = data
        else:
            logger.warning(f"Processing {total_points} points in batches of {BATCH_SIZE} with {MAX_WORKERS} workers")
            
            batches = [points_list[i:i + BATCH_SIZE] for i in range(0, total_points, BATCH_SIZE)]
            
            for batch_idx, batch in enumerate(batches):
                logger.warning(f"Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} points)")
                
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
                    futures = {
                        executor.submit(_fetch_single_point, address, token, arg_point, point, fetch_function): point
                        for point in batch
                    }
                    
                    for future in as_completed(futures):
                        point_name, data = future.result()
                        if point_name and data is not None:
                            data_points[point_name] = data
        
        if not data_points:
            logger.warning(f"No data collected from {total_points} points")
            return pd.DataFrame()
        
        logger.warning(f"Successfully collected data from {len(data_points)}/{total_points} points")
        return pd.DataFrame(data_points)
        
    except Exception as e:
        logger.error(f"Failed to get points dataframe: {e}", exc_info=True)
        return pd.DataFrame()


def check_and_get_token(id_farm):
    try:
        smart_his = SmartHIS.objects.filter(farm_id=id_farm).first()
        if not smart_his:
            logger.error(f"SmartHIS not found for farm {id_farm}")
            return None
        
        conn = get_connection(smart_his.url)
        if conn is None:
            logger.error(f"Failed to connect to {smart_his.url} for farm {id_farm}")
            return None
        
        try:
            check_response = get_current(conn, smart_his.token, smart_his.point_check_expired)
            
            if check_response.status == 200:
                return smart_his.token
            elif check_response.status == 401:
                logger.warning(f"Token expired for farm {id_farm}, refreshing token")
                refresh_token = login_and_get_token(
                    smart_his.address,
                    smart_his.username,
                    smart_his.password
                )
                if refresh_token:
                    smart_his.token = refresh_token
                    smart_his.save()
                    logger.warning(f"Token refreshed successfully for farm {id_farm}")
                    return smart_his.token
                else:
                    logger.error(f"Failed to refresh token for farm {id_farm}")
                    return None
            else:
                logger.error(f"Token check failed for farm {id_farm}: status {check_response.status}, reason {check_response.reason}")
                return None
        finally:
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Error closing connection in check_and_get_token: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error in check_and_get_token for farm {id_farm}: {e}", exc_info=True)
        return None