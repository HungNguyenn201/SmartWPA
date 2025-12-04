import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from functools import reduce

TIME_RESOLUTION = pd.Timedelta(minutes=10)

all_statuses = [
    'NORMAL',
    'MEASUREMENT_ERROR',
    'STOP',
    'PARTIAL_STOP',
    'CURTAILMENT',
    'PARTIAL_CURTAILMENT',
    'OVERPRODUCTION',
    'UNDERPRODUCTION',
    'UNKNOWN',
]

def filter_error(data: pd.DataFrame, constants: dict, CUT_MARGIN: float = 1):
    conditions = [
        data['ACTIVE_POWER'] <= 0,
        data['WIND_SPEED'] < 0
    ]
    choices = ['STOP', 'MEASUREMENT_ERROR']

    data['status'] = np.select(conditions, choices, default='UNKNOWN')
    data['status'] = pd.Categorical(data['status'], categories=all_statuses, ordered=False)

    #Filter out erroneously measured data.

    #All row with wind speed outside [0, 32] m/s, or power outside [-0.05 * P_rated, 1.1 * P_rated] is classified as errors.
    data.loc[(data['WIND_SPEED'] < 0) | (data['WIND_SPEED'] > 32) | (data['ACTIVE_POWER'] < -0.05 * constants['P_rated']) | (data['ACTIVE_POWER'] > 1.1 * constants['P_rated']), 'status'] = 'MEASUREMENT_ERROR' 

    #Mark all rows with null wind speed or power, or have power while wind speed is smaller than the cut-in speed as errors.
    data.loc[data['ACTIVE_POWER'].isnull() | data['WIND_SPEED'].isnull(), 'status'] = 'MEASUREMENT_ERROR'
    data.loc[((data['WIND_SPEED'] < constants['V_cutin'] - CUT_MARGIN) | (data['WIND_SPEED'] > constants['V_cutout'] + CUT_MARGIN)) & data['ACTIVE_POWER'] > 0, 'status'] = 'MEASUREMENT_ERROR'
    
    #If wind speed differ from the previous rows by more than 10 m/s, assume error.
    data.loc[data['WIND_SPEED'].diff().abs() > 10, ['status', 'WIND_SPEED', 'ACTIVE_POWER']] = ['MEASUREMENT_ERROR', np.nan, np.nan]

    #If during a minimum one-hour span, the measured wind speed is constant, assume they're dead values and mark as errors.
    tmp = data[data['status'] == 'UNKNOWN']
    groups = tmp.groupby((tmp['WIND_SPEED'] != tmp['WIND_SPEED'].shift()).cumsum())
    error_indices = []
    for _, group in groups:
        if len(group) >= 6:
            if (np.all(np.diff(group.index) == 1)):
                error_indices.append(group.index.to_numpy()[1:])

    if error_indices:
        errors = np.concatenate(error_indices)
        data.loc[errors, 'status'] = 'MEASUREMENT_ERROR'
    return data

def estimate_eps(scaled_data, min_samples):
    nbrs = NearestNeighbors(n_neighbors=min_samples).fit(scaled_data)
    distances, indices = nbrs.kneighbors(scaled_data)
    
    sorted_distances = np.sort(distances[:, min_samples-1], axis=0)
    
    n_points = len(sorted_distances)
    all_coords = np.vstack((range(n_points), sorted_distances)).T
    
    first_point = all_coords[0]
    last_point = all_coords[-1]
    line_vec = last_point - first_point
    line_vec_norm = np.linalg.norm(line_vec)
    
    vec_from_first = all_coords - first_point
    scalar_product = np.sum(vec_from_first * np.tile(line_vec, (n_points, 1)), axis=1)
    vec_on_line = np.outer(scalar_product / line_vec_norm**2, line_vec)
    vec_to_line = vec_from_first - vec_on_line
    dist_to_line = np.linalg.norm(vec_to_line, axis=1)
    
    knee_idx = np.argmax(dist_to_line)
    optimal_eps = sorted_distances[knee_idx]
    
    return optimal_eps

def run_dbscan(df, features, min_samples, eps=None):    
    global_scaler = StandardScaler()
    scaled_sample = global_scaler.fit_transform(df[features])
    
    if eps == None:
        eps = estimate_eps(scaled_sample, min_samples)
    
    from sklearn.cluster import DBSCAN
    HAS_CUML = False
    chunk_size = 25000

    overlap = chunk_size // 10

    inliers_list = []
    num_rows = len(df)
    
    for start_idx in range(0, num_rows, chunk_size):
        core_end = min(start_idx + chunk_size, num_rows)
        padded_start = max(0, start_idx - overlap)
        padded_end = min(num_rows, core_end + overlap)
        
        padded_chunk_df = df.iloc[padded_start:padded_end]
        if len(padded_chunk_df) == 0: continue
            
        padded_scaled = global_scaler.transform(padded_chunk_df[features])

        if HAS_CUML:
            db = DBSCAN(eps=eps, min_samples=min_samples)
        else:
            db = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)

        padded_labels = db.fit_predict(padded_scaled)
        
        valid_start_rel = start_idx - padded_start
        valid_end_rel = valid_start_rel + (core_end - start_idx)
        core_labels = padded_labels[valid_start_rel:valid_end_rel]
        core_df = df.iloc[start_idx:core_end]

        inlier_mask = core_labels != -1
        if np.any(inlier_mask):
            inliers_list.append(core_df[inlier_mask])

    if inliers_list:
        final_inliers_df = pd.concat(inliers_list)
    else:
        final_inliers_df = pd.DataFrame(columns=df.columns)
    
    return final_inliers_df

FEATURES = ['WIND_SPEED', 'ACTIVE_POWER']
MIN_SAMPLES = 15
LEAST_TIME_OF_CURTAILMENT = 30
LEAST_TIME_OF_STOP = 240
LEAST_TIME_OF_NORMAL = 40
MAX_DIFF_IN_CURTAILMENT = 100

def power_curve_regression(normal_data, bin_width):
    normal_data = normal_data.copy()

    wind_speed = normal_data['WIND_SPEED']
    bins = np.arange(wind_speed.min(), wind_speed.max(), bin_width)
    
    normal_data['bin'] = pd.cut(wind_speed, bins=bins, labels=False, include_lowest=True)
    centers = normal_data.groupby('bin')['WIND_SPEED'].mean()

    final_stats = normal_data.groupby('bin')['ACTIVE_POWER'].agg(
        median='median'
    )

    centers_df = centers.reset_index(name='center')
    final_curve_df = pd.merge(centers_df, final_stats, on='bin', how='left')

    final_curve_df = final_curve_df.ffill().bfill()

    return final_curve_df

def find_healthy_area_band(df, estimated_curve, 
                           step_size=10, 
                           stop_threshold=0.002, 
                           max_band=1000):    
    df['lower_dev'] = (df['THEORETICAL_POWER'] - df['ACTIVE_POWER'])
    df['upper_dev'] = (df['ACTIVE_POWER'] - df['THEORETICAL_POWER'])

    lower_data = df[df['lower_dev'] >= 0]
    upper_data = df[df['upper_dev'] >= 0]

    band_edges = np.arange(step_size, max_band + step_size, step_size)

    final_lower_band = max_band
    n_lower_points = len(lower_data)
    if n_lower_points > 0:
        sorted_lower_devs = np.sort(lower_data['lower_dev'].to_numpy())
        
        points_in_band = np.searchsorted(sorted_lower_devs, band_edges, side='right')
        new_points = np.diff(points_in_band, prepend=0)
        points_in_last_band = points_in_band - new_points
        
        relative_change = np.full_like(new_points, np.inf, dtype=float)
        mask = points_in_last_band > 0
        relative_change[mask] = new_points[mask] / points_in_last_band[mask]

        stop_idx_arr = np.where(relative_change < stop_threshold)[0]
        if stop_idx_arr.size > 0:
            final_lower_band = band_edges[stop_idx_arr[0]]
        elif final_lower_band == max_band:
             print(f"Warning: Lower bound reached max_band ({max_band}).")
    
    final_upper_band = max_band
    n_upper_points = len(upper_data)
    if n_upper_points > 0:
        sorted_upper_devs = np.sort(upper_data['upper_dev'].to_numpy())
        
        points_in_band = np.searchsorted(sorted_upper_devs, band_edges, side='right')
        new_points = np.diff(points_in_band, prepend=0)
        points_in_last_band = points_in_band - new_points
        
        relative_change = np.full_like(new_points, np.inf, dtype=float)
        mask = points_in_last_band > 0
        relative_change[mask] = new_points[mask] / points_in_last_band[mask]

        stop_idx_arr = np.where(relative_change < stop_threshold)[0]
        if stop_idx_arr.size > 0:
            final_upper_band = band_edges[stop_idx_arr[0]]
        elif final_upper_band == max_band:
            print(f"Warning: Upper bound reached max_band ({max_band}).")

    estimated_curve['lower'] = estimated_curve['median'] - final_lower_band
    estimated_curve['upper'] = estimated_curve['median'] + final_upper_band
    
    return estimated_curve

def status_mapper(row):
    if row['status'] != 'UNKNOWN':
        return row['status']
    
    val = row['ACTIVE_POWER']
    if val < row['lower']:
        return 'UNDERPRODUCTION'
    elif val > row['upper']:
        return 'OVERPRODUCTION'
    else:
        return 'NORMAL'

def classify_performance(data, estimated_curve):
    data['THEORETICAL_POWER'] = CubicSpline(estimated_curve['center'], estimated_curve['median'])(data['WIND_SPEED'])
    estimated_curve = find_healthy_area_band(data, estimated_curve)

    data['lower'] = CubicSpline(estimated_curve['center'], estimated_curve['lower'])(data['WIND_SPEED'])
    data['upper'] = CubicSpline(estimated_curve['center'], estimated_curve['upper'])(data['WIND_SPEED'])

    unknown_mask = data['status'] == 'UNKNOWN'
    
    underprod_mask = unknown_mask & (data['ACTIVE_POWER'] < data['lower'])
    overprod_mask = unknown_mask & (data['ACTIVE_POWER'] > data['upper'])
    normal_mask = unknown_mask & (~underprod_mask) & (~overprod_mask)

    data['status'] = data['status'].mask(underprod_mask, 'UNDERPRODUCTION')
    data['status'] = data['status'].mask(overprod_mask, 'OVERPRODUCTION')
    data['status'] = data['status'].mask(normal_mask, 'NORMAL')

    data = data.drop(columns=['lower', 'upper', 'lower_dev', 'upper_dev', 'THEORETICAL_POWER'])
    return data

def classify_curtailment(classified):
    is_underprod = (classified['status'] == 'UNDERPRODUCTION')

    window_size_W = np.int32(pd.Timedelta(minutes=LEAST_TIME_OF_CURTAILMENT) / TIME_RESOLUTION)
    rolling_std = classified['ACTIVE_POWER'].rolling(window=window_size_W, min_periods=1).std()

    is_stable = rolling_std < MAX_DIFF_IN_CURTAILMENT

    is_potential_curtailment = is_underprod & is_stable

    temp_group_id = is_potential_curtailment.ne(is_potential_curtailment.shift()).cumsum()

    potential_groups_df = classified[is_potential_curtailment].copy()
    potential_groups_df['temp_group'] = temp_group_id[is_potential_curtailment]

    curtailment_groups = potential_groups_df.groupby('temp_group')

    stable_groups = []
    least_time = pd.Timedelta(minutes=LEAST_TIME_OF_CURTAILMENT)

    for group_id, group in curtailment_groups:
        if len(group) > 1:
            duration = group.index[-1] - group.index[0]
            if duration >= least_time:
                stable_groups.append((group.index.min(), group.index.max()))

    curtailment_mask = pd.Series(False, index=classified.index)

    for start_idx, end_idx in stable_groups:
        curtailment_mask.loc[start_idx:end_idx] = True

    classified.loc[curtailment_mask, 'status'] = 'CURTAILMENT'

    return classified, stable_groups

def closest_normal_group_from(data, index_front, index_back, least_time_minutes):
    int_front = data.index.searchsorted(index_front)
    int_back = data.index.searchsorted(index_back)

    normals = data['status'] == 'NORMAL'
    groups_mask = normals.ne(normals.shift()).cumsum()
    
    normal_rows = data[normals].copy()
    normal_rows['group_id'] = groups_mask[normals]

    normal_rows['iloc_idx'] = np.arange(len(data))[normals]

    minimum_size = pd.Timedelta(minutes=least_time_minutes) / TIME_RESOLUTION
    
    sizes = normal_rows.groupby('group_id').size()
    valid_group_ids = sizes[sizes >= minimum_size].index
    
    valid_rows = normal_rows[normal_rows['group_id'].isin(valid_group_ids)]

    if valid_rows.empty:
        return None

    boundaries = valid_rows.groupby('group_id').agg(
        start_iloc = ('iloc_idx', 'min'),
        end_iloc = ('iloc_idx', 'max')
    ).reset_index(drop=True)

    groups_front = boundaries[boundaries['end_iloc'] < int_front].copy()
    result_front = None
    
    if not groups_front.empty:
        groups_front['distance'] = int_front - groups_front['end_iloc']
        closest_front = groups_front.loc[groups_front['distance'].idxmin()]
        result_front = int(closest_front['end_iloc'])
    
    groups_back = boundaries[boundaries['start_iloc'] > int_back].copy()
    result_back = None
    
    if not groups_back.empty:
        groups_back['distance'] = groups_back['start_iloc'] - int_back
        closest_back = groups_back.loc[groups_back['distance'].idxmin()]
        result_back = int(closest_back['start_iloc'])

    return (result_front, result_back)

def classify_partial_curtailment(classified, stable_groups):
    indices_to_update = []
    
    for curtailment_group in stable_groups:
        c_start_ts, c_end_ts = curtailment_group[0], curtailment_group[1]
        c_start_iloc = classified.index.searchsorted(c_start_ts)
        c_end_iloc = classified.index.searchsorted(c_end_ts)

        res = closest_normal_group_from(
            classified, 
            c_start_ts, 
            c_end_ts, 
            least_time_minutes=LEAST_TIME_OF_NORMAL
        )
        
        if res:
            front_iloc, back_iloc = res
            if front_iloc is not None:
                subset_front = classified.iloc[front_iloc : c_start_iloc]
                
                if not subset_front.empty and (subset_front['status'] != 'CURTAILMENT').all():
                    indices_to_update.append(subset_front.index)

            if back_iloc is not None:               
                subset_back = classified.iloc[c_end_iloc + 1 : back_iloc + 1]
                
                if not subset_back.empty and (subset_back['status'] != 'CURTAILMENT').all():
                    indices_to_update.append(subset_back.index)

    if indices_to_update:
        all_indices = reduce(lambda x, y: x.union(y), indices_to_update)
        
        classified.loc[all_indices, 'status'] = 'PARTIAL_CURTAILMENT'
        
    return classified

def consecutive_stop(data, least_time_minutes):
    stops = data['status'] == 'STOP'
    groups_mask = stops.ne(stops.shift()).cumsum()
    stop_rows = data[stops].copy()
    stop_rows['group_id'] = groups_mask[stops]

    minimum_size = pd.Timedelta(minutes=least_time_minutes) / TIME_RESOLUTION
    sizes = stop_rows.groupby('group_id').size()
    valid_group_ids = sizes[sizes >= minimum_size].index
    
    groups = stop_rows[stop_rows['group_id'].isin(valid_group_ids)]

    if groups.empty:
        return []

    boundaries = groups.reset_index()
    time_col = boundaries.columns[0] 
    
    group_boundaries = boundaries.groupby('group_id')[time_col].agg(
        start_ts='min', 
        end_ts='max'
    ).reset_index()
    
    return list(zip(group_boundaries['start_ts'], group_boundaries['end_ts']))


def classify_partial_stops(classified):
    stop_groups = consecutive_stop(classified, least_time_minutes=LEAST_TIME_OF_STOP)
    
    indices_to_update = []
    valid_statuses = ['NORMAL', 'UNDERPRODUCTION', 'OVERPRODUCTION']
    
    for stop_group in stop_groups:
        s_start_ts, s_end_ts = stop_group[0], stop_group[1]
        
        res = closest_normal_group_from(
            classified, 
            s_start_ts, 
            s_end_ts, 
            least_time_minutes=LEAST_TIME_OF_NORMAL
        )
        
        if res:
            front_iloc, back_iloc = res
            s_start_iloc = classified.index.searchsorted(s_start_ts)
            s_end_iloc = classified.index.searchsorted(s_end_ts)
            
            if front_iloc is not None:
                subset_front = classified.iloc[front_iloc : s_start_iloc]
                
                if not subset_front.empty:
                    if subset_front['status'].isin(valid_statuses).all():
                        indices_to_update.append(subset_front.index)

            if back_iloc is not None:
                subset_back = classified.iloc[s_end_iloc + 1 : back_iloc + 1]
                
                if not subset_back.empty:
                    if subset_back['status'].isin(valid_statuses).all():
                        indices_to_update.append(subset_back.index)

    if indices_to_update:
        all_indices = reduce(lambda x, y: x.union(y), indices_to_update)
        classified.loc[all_indices, 'status'] = 'PARTIAL_STOP'

    classified['status'] = classified['status'].mask(classified['ACTIVE_POWER'] <= 0, 'STOP')
    
    return classified

def classify(raw_data: pd.DataFrame, constants: dict) -> pd.DataFrame:
    raw_data = raw_data.sort_index()

    filtered_data = filter_error(raw_data, constants)

    nerror_data = filtered_data[filtered_data['status'] == 'UNKNOWN']

    df_no_outliers = run_dbscan(
        df=nerror_data, 
        features=FEATURES,
        min_samples=MIN_SAMPLES,
        eps=0.2
    )

    estimated_curve = power_curve_regression(
        normal_data=df_no_outliers,
        bin_width=0.25
    )
    
    classified_data = classify_performance(filtered_data, estimated_curve)
    classified_data, stable_groups = classify_curtailment(classified_data)
    classified_data = classify_partial_curtailment(classified_data, stable_groups)
    classified_data = classify_partial_stops(classified_data)

    return classified_data

def classification_to_obj(data: pd.DataFrame) -> object:
    obj = {}
    data = data.copy()
    obj['classification_map'] = {index: element for index, element in enumerate(data['status'].cat.categories[:-1])}

    data['status'] = data['status'].cat.codes
    obj['classification_rates'] = data['status'].value_counts().sort_index().to_dict()

    data.index = data.index.astype(int)
    obj['classification_points'] = data[['WIND_SPEED', 'ACTIVE_POWER', 'status']].rename(columns={'status': 'classification'}).to_dict('split')

    return obj