"""
Farm-level Cross turbine analysis API (manual 1.3.5.3 b).

POST /api/farms/{farm_id}/cross-data-analysis/
- Same request schema as turbine cross-data, plus: turbine_ids, max_points_per_turbine. Luôn trả curve (binned) khi X≠wind_direction, wind_rose khi X=wind_direction; không có tham số output.
- When x_source == wind_direction, response includes wind_rose (sector aggregation).
- group_by=turbine allowed; response points include turbine_id.
"""
import logging
from typing import Any, Dict, List, Optional

from django.core.cache import cache
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication

from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers import cross_data_analysis_helpers as x_helpers
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers._header import (
    CROSS_ANALYSIS_CACHE_TIMEOUT_SECONDS,
    CROSS_ANALYSIS_GROUP_BY_VALUES,
    CROSS_ANALYSIS_STATUS_BY_CODE,
    CROSS_ANALYSIS_SOURCE_GROUP_BINS_DEFAULT,
    CROSS_ANALYSIS_SOURCE_GROUP_BINS_MIN,
    CROSS_ANALYSIS_SOURCE_GROUP_BINS_MAX,
    CROSS_ANALYSIS_MAX_POINTS_MIN,
    CROSS_ANALYSIS_MAX_POINTS_MAX,
    CROSS_ANALYSIS_SECTORS_NUMBER_DEFAULT,
    CROSS_ANALYSIS_CURVE_BINS_DEFAULT,
)
from api_gateway.turbines_analysis.helpers.timeseries_helpers import SOURCE_TO_FIELD_MAPPING
from facilities.models import Turbines, Farm
from permissions.views import CanViewFarm

logger = logging.getLogger("api_gateway.turbines_analysis")

VALID_SOURCES = set(SOURCE_TO_FIELD_MAPPING.keys())
# Farm allows group_by=turbine; all other group_by values same as turbine
VALID_GROUP_BY_FARM = CROSS_ANALYSIS_GROUP_BY_VALUES
MAX_POINTS_PER_TURBINE_DEFAULT = 10_000
MAX_POINTS_PER_TURBINE_MAX = 50_000


def _normalize_day_night(value: Any) -> str:
    """Unify day_night to 'day' | 'night' | ''."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    if s in ("day", "night"):
        return s
    return ""


def _parse_farm_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Parse farm cross-analysis payload. Reuses same base as turbine; adds farm-specific and normalizes day_night."""
    from api_gateway.turbines_analysis.cross_data_analysis import _parse_payload
    params = _parse_payload(payload)
    params["day_night"] = _normalize_day_night(params.get("day_night"))
    # Farm-specific
    turbine_ids_raw = payload.get("turbine_ids") or []
    try:
        turbine_ids = [int(x) for x in turbine_ids_raw] if isinstance(turbine_ids_raw, list) else []
    except (TypeError, ValueError):
        turbine_ids = []
    params["turbine_ids"] = turbine_ids
    max_ppt = payload.get("max_points_per_turbine")
    if max_ppt is not None:
        try:
            max_ppt = max(100, min(MAX_POINTS_PER_TURBINE_MAX, int(max_ppt)))
        except (TypeError, ValueError):
            max_ppt = MAX_POINTS_PER_TURBINE_DEFAULT
    else:
        max_ppt = MAX_POINTS_PER_TURBINE_DEFAULT
    params["max_points_per_turbine"] = max_ppt
    return params


def _run_farm_pipeline(
    farm: Farm,
    turbines: List[Turbines],
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Run cross-analysis for farm: load SCADA, apply filters, build points or wind_rose."""
    x_source = params["x_source"]
    y_source = params["y_source"]
    group_by = params["group_by"]
    start_ms = int(params["start_time"]) if params["start_time"] is not None else None
    end_ms = int(params["end_time"]) if params["end_time"] is not None else None
    start_hour = params["start_hour"]
    end_hour = params["end_hour"]
    max_points = x_helpers.clamp_max_points(params["max_points"])
    months = params["months"]
    day_night = params["day_night"]
    direction_filter = params["direction_filter"]
    ranges = params["ranges"]
    only_comp = params["only_computation_data"]
    classifications = params["classifications"]
    group_source = params["group_source"]
    include_statistics = params["include_statistics"]
    max_points_per_turbine = params["max_points_per_turbine"]

    needed_sources = {x_source, y_source}
    direction_source, sectors_number, sector_ids = x_helpers.direction_filter_to_params(
        direction_filter, VALID_SOURCES
    )
    if direction_source:
        needed_sources.add(direction_source)
    for r in ranges:
        src = r.get("source")
        if src and src in VALID_SOURCES:
            needed_sources.add(src)
    if group_by == "source" and group_source and group_source in VALID_SOURCES:
        needed_sources.add(group_source)
    need_classification = group_by == "classification" or bool(classifications)

    df_raw, _data_source_used, _error_info = x_helpers.load_farm_scada_for_cross_analysis(
        turbines, start_ms, end_ms, needed_sources, SOURCE_TO_FIELD_MAPPING, max_points_per_turbine
    )
    if df_raw is None or df_raw.empty:
        return None

    # NOTE:
    # load_farm_scada_for_cross_analysis() already:
    # - selects needed columns
    # - renames SCADA fields -> source names (e.g. ACTIVE_POWER -> power)
    # - adds timestamp_ms + turbine_id
    # Re-selecting with SOURCE_TO_FIELD_MAPPING here would drop renamed columns and can lead to NO_DATA.
    df = df_raw
    before_count = int(len(df))

    ts_dt = x_helpers.ensure_time_columns(df)
    df = x_helpers.apply_time_filters(df, start_hour, end_hour, months, day_night)
    if direction_source and sector_ids:
        df = x_helpers.apply_direction_filter(df, direction_source, sectors_number, sector_ids)
    df = x_helpers.apply_range_filters(df, ranges)
    df = x_helpers.build_xy_and_drop_invalid(df, x_source, y_source)

    if need_classification:
        cdf = x_helpers.fetch_classification_for_farm(
            turbines, start_ms, end_ms, CROSS_ANALYSIS_STATUS_BY_CODE
        )
        if cdf is not None and not cdf.empty:
            df = df.merge(cdf, on=["timestamp_ms", "turbine_id"], how="left")
            df["group"] = df["group"].fillna("UNKNOWN")
        else:
            df["group"] = "UNKNOWN"
        if classifications:
            df = df[df["group"].isin(classifications)]

    after_count = int(len(df))
    if df.empty:
        return None

    group_series = None
    if group_by == "classification" and "group" in df.columns:
        group_series = df["group"]
    elif group_by == "turbine":
        # Use turbine_id for group label; build_points_list will add turbine_id to each point
        if "turbine_id" in df.columns:
            turbine_names = {t.id: t.name for t in turbines}
            df["group"] = df["turbine_id"].map(lambda i: turbine_names.get(i, f"WT{i}"))
            group_series = df["group"]
    elif group_by == "source" and group_source in df.columns:
        group_series = x_helpers.bin_source_values(
            df, group_source, params["group_count"], params["group_min"], params["group_max"]
        )
        if group_series is not None:
            df["group"] = group_series.values
    elif group_by in ("monthly", "yearly", "seasonally"):
        group_series = x_helpers.get_temporal_group_series(df, ts_dt, group_by)
        if group_series is not None:
            df["group"] = group_series.values

    statistics = None
    if include_statistics and len(df) > 0:
        statistics = x_helpers.compute_xy_statistics(df, "x", "y")

    is_wind_rose = x_source == "wind_direction"
    wind_rose = None
    points: List[Dict[str, Any]] = []

    if is_wind_rose:
        sectors_number_rose = sectors_number or CROSS_ANALYSIS_SECTORS_NUMBER_DEFAULT
        sectors_number_rose = max(1, min(72, sectors_number_rose))
        sectors_list, by_turbine_list = x_helpers.compute_wind_rose_sectors(
            df, sectors_number_rose, direction_source=x_source, y_col="y",
            turbine_id_col="turbine_id" if group_by == "turbine" and "turbine_id" in df.columns else None,
        )
        wind_rose = {
            "sectors_number": sectors_number_rose,
            "direction_source": x_source,
            "sectors": sectors_list,
        }
        if by_turbine_list:
            turbine_names = {t.id: t.name for t in turbines}
            for bt in by_turbine_list:
                bt["turbine_name"] = turbine_names.get(bt["turbine_id"], f"WT{bt['turbine_id']}")
            wind_rose["by_turbine"] = by_turbine_list
    else:
        points = x_helpers.compute_binned_curve_points(
            df,
            x_col="x",
            y_col="y",
            group_col="group" if group_series is not None else None,
            turbine_id_col="turbine_id" if "turbine_id" in df.columns else None,
            n_bins=CROSS_ANALYSIS_CURVE_BINS_DEFAULT,
        )

    turbine_id_list = [t.id for t in turbines]
    summary = {
        "rows_before_filters": before_count,
        "rows_after_filters": after_count,
        "points_returned": len(points),
        "turbines_requested": turbine_id_list,
        "turbine_count": len(turbine_id_list),
    }

    result = {
        "farm_id": farm.id,
        "farm_name": farm.name,
        "x_source": x_source,
        "y_source": y_source,
        "group_by": group_by,
        "period": {"start_time_ms": start_ms, "end_time_ms": end_ms},
        "summary": summary,
        "points": points,
    }
    if wind_rose is not None:
        result["wind_rose"] = wind_rose
    if statistics is not None:
        result["statistics"] = statistics
    return result


class FarmCrossDataAnalysisAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewFarm]

    def post(self, request, farm_id=None):
        try:
            farm_id = farm_id or request.data.get("farm_id") or request.query_params.get("farm_id")
            if not farm_id:
                return error_response("Farm ID must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            try:
                farm = Farm.objects.select_related("investor").get(id=farm_id)
            except Farm.DoesNotExist:
                return error_response("Farm not found", "FARM_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            permission_response = check_object_permission(
                request, self, farm, "You don't have permission to access this farm"
            )
            if permission_response:
                return permission_response

            turbines = Turbines.objects.filter(farm=farm).select_related("farm", "farm__investor")
            if not turbines.exists():
                return error_response("No turbines in this farm", "NO_TURBINES", status.HTTP_404_NOT_FOUND)

            payload = request.data or {}
            params = _parse_farm_payload(payload)
            turbine_ids_req = params.get("turbine_ids") or []
            if turbine_ids_req:
                turbines = [t for t in turbines if t.id in turbine_ids_req]
                if not turbines:
                    return error_response(
                        "No matching turbines for the given turbine_ids",
                        "INVALID_PARAMETERS",
                        status.HTTP_400_BAD_REQUEST,
                    )
            else:
                turbines = list(turbines)

            if params["x_source"] not in VALID_SOURCES or params["y_source"] not in VALID_SOURCES:
                return error_response(
                    f"x_source and y_source must be in: {', '.join(sorted(VALID_SOURCES))}",
                    "INVALID_PARAMETERS",
                    status.HTTP_400_BAD_REQUEST,
                )
            if params["group_by"] not in VALID_GROUP_BY_FARM:
                return error_response(
                    f"group_by must be one of: {', '.join(sorted(VALID_GROUP_BY_FARM))}",
                    "INVALID_PARAMETERS",
                    status.HTTP_400_BAD_REQUEST,
                )

            payload_for_cache = {k: v for k, v in (payload or {}).items() if k != "regression"}
            cache_key = x_helpers.get_cross_analysis_cache_key("farm_cross_data", int(farm.id), payload_for_cache)
            cached = cache.get(cache_key)
            if cached:
                return success_response(cached)

            result = _run_farm_pipeline(farm, turbines, params)
            if result is None:
                return error_response(
                    "No data available for the specified time range and turbines",
                    "NO_DATA",
                    status.HTTP_404_NOT_FOUND,
                )

            cache.set(cache_key, result, timeout=CROSS_ANALYSIS_CACHE_TIMEOUT_SECONDS)
            return success_response(result)

        except Exception as e:
            logger.error("Error in FarmCrossDataAnalysisAPIView.post: %s", str(e), exc_info=True)
            return error_response(
                "An unexpected error occurred",
                "INTERNAL_SERVER_ERROR",
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
