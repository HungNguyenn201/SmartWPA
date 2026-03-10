"""
Cross-data analysis API: turbine-level only (per manual 1.3.6.2.7).

Uses helpers for all computation; constants from _header.
"""
import logging
from typing import Any, Dict, List, Optional

from django.core.cache import cache
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
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
)
from api_gateway.turbines_analysis.helpers.computation_helper import load_turbine_data
from api_gateway.turbines_analysis.helpers.timeseries_helpers import SOURCE_TO_FIELD_MAPPING
from facilities.models import Turbines
from permissions.views import CanViewTurbine
from analytics.models import Computation, ClassificationPoint

logger = logging.getLogger("api_gateway.turbines_analysis")

VALID_SOURCES = set(SOURCE_TO_FIELD_MAPPING.keys())
VALID_GROUP_BY = CROSS_ANALYSIS_GROUP_BY_VALUES - {"turbine"}


def _parse_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and validate common cross-analysis params from payload. Returns plain dict."""
    dt = payload.get("datetime") or {}
    filters = payload.get("filters") or {}
    months_raw = filters.get("months") or []
    try:
        months = [int(m) for m in months_raw if 1 <= int(m) <= 12]
    except (TypeError, ValueError):
        months = []

    # Regression: no longer read from request; server always returns all types (see plan).
    group_cfg = payload.get("group") or {}
    classifications_raw = filters.get("classifications") or []
    classifications = [str(c).upper() for c in classifications_raw if c]

    group_source = (group_cfg.get("source") or "").lower()
    group_count = group_cfg.get("groups_count")
    if group_count is not None:
        group_count = max(CROSS_ANALYSIS_SOURCE_GROUP_BINS_MIN,
                          min(CROSS_ANALYSIS_SOURCE_GROUP_BINS_MAX, int(group_count)))
    else:
        group_count = CROSS_ANALYSIS_SOURCE_GROUP_BINS_DEFAULT

    return {
        "x_source": payload.get("x_source"),
        "y_source": payload.get("y_source"),
        "group_by": ({
            "time_profile_monthly": "monthly",
            "time_profile_seasonally": "seasonally",
        }.get((payload.get("group_by") or "none").lower(), (payload.get("group_by") or "none").lower())),
        "start_time": dt.get("start_time_ms"),
        "end_time": dt.get("end_time_ms"),
        "start_hour": dt.get("start_hour"),
        "end_hour": dt.get("end_hour"),
        "max_points": payload.get("max_points"),
        "months": months,
        "day_night": (filters.get("day_night") or "").lower(),
        "direction_filter": filters.get("direction") or {},
        "ranges": filters.get("ranges") or [],
        "filters": filters,
        "only_computation_data": bool(payload.get("only_computation_data", False)),
        "classifications": classifications,
        "group_source": group_source,
        "group_count": group_count,
        "group_min": group_cfg.get("min"),
        "group_max": group_cfg.get("max"),
        "include_statistics": bool(payload.get("include_statistics", False)),
    }


def _run_turbine_pipeline(
    turbine: Turbines,
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Run full cross-analysis pipeline for one turbine. Returns result dict or None on no data."""
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
    need_classification = (
        group_by == "classification" or bool(classifications)
    )

    df_raw = None
    data_source_used = None
    error_info = {}
    units_meta = None

    use_fast_path = only_comp or needed_sources.issubset({"power", "wind_speed"})
    if use_fast_path:
        try:
            comp_q = Computation.objects.filter(
                turbine=turbine, computation_type="classification", is_latest=True
            )
            if start_ms is not None and end_ms is not None:
                comp = comp_q.filter(start_time=start_ms, end_time=end_ms).first()
                if comp is None:
                    comp = comp_q.order_by("-end_time").first()
                    if comp and not (int(comp.start_time) <= int(start_ms) and int(comp.end_time) >= int(end_ms)):
                        comp = None
            else:
                comp = comp_q.order_by("-end_time").first()
            if comp is not None:
                pts = ClassificationPoint.objects.filter(computation=comp).only(
                    "timestamp", "wind_speed", "active_power"
                )
                if start_ms is not None:
                    pts = pts.filter(timestamp__gte=int(start_ms))
                if end_ms is not None:
                    pts = pts.filter(timestamp__lte=int(end_ms))
                rows = list(pts.values_list("timestamp", "wind_speed", "active_power"))
                if rows:
                    import pandas as _pd
                    df_raw = _pd.DataFrame(rows, columns=["TIMESTAMP", "WIND_SPEED", "ACTIVE_POWER"])
                    data_source_used = "classification_points"
                    units_meta = {
                        "canonical": {"WIND_SPEED": "m/s", "ACTIVE_POWER": "kW"},
                        "raw_config": {"config_scope": "classification_points", "config_id": None, "data_source": "db"},
                    }
        except Exception as e:
            logger.debug("Turbine cross-data: classification points fast-path failed: %s", str(e))

    if df_raw is None:
        if only_comp:
            return None
        df_raw, data_source_used, error_info, units_meta = load_turbine_data(
            turbine, start_ms, end_ms, preferred_source="db"
        )
    if df_raw is None or df_raw.empty:
        return None

    df = x_helpers.select_and_rename_columns(df_raw, needed_sources, SOURCE_TO_FIELD_MAPPING)
    x_helpers.normalize_timestamp_ms(df)
    before_count = int(len(df))

    ts_dt = x_helpers.ensure_time_columns(df)
    # Advanced filters order per manual 1.3.6.2.7: Month, Day/Night, Direction, Source (min/max)
    df = x_helpers.apply_time_filters(df, start_hour, end_hour, months, day_night)
    if direction_source and sector_ids:
        df = x_helpers.apply_direction_filter(df, direction_source, sectors_number, sector_ids)
    df = x_helpers.apply_range_filters(df, ranges)
    df = x_helpers.build_xy_and_drop_invalid(df, x_source, y_source)

    if need_classification:
        cdf = x_helpers.fetch_classification_for_turbine(
            turbine, start_ms, end_ms, CROSS_ANALYSIS_STATUS_BY_CODE
        )
        if cdf is not None and not cdf.empty:
            df = df.merge(cdf, on="timestamp_ms", how="left")
            df["group"] = df["group"].fillna("UNKNOWN")
        else:
            df["group"] = "UNKNOWN"
        if classifications:
            df = df[df["group"].isin(classifications)]

    after_count = int(len(df))

    group_series = None
    if group_by == "classification" and "group" in df.columns:
        group_series = df["group"]
    elif group_by == "source" and group_source in df.columns:
        group_series = x_helpers.bin_source_values(
            df, group_source, params["group_count"], params["group_min"], params["group_max"]
        )
        if group_series is not None:
            df["group"] = group_series.values
    elif group_by in ("monthly", "yearly", "seasonally", "time_profile_monthly", "time_profile_seasonally"):
        group_series = x_helpers.get_temporal_group_series(df, ts_dt, group_by)
        if group_series is not None:
            df["group"] = group_series.values

    statistics = None
    if include_statistics and len(df) > 0:
        statistics = x_helpers.compute_xy_statistics(df, "x", "y")

    df = x_helpers.downsample_to_max_points(df, max_points)
    points = x_helpers.build_points_list(df, group_col="group" if group_series is not None else None, turbine_id_col=None)

    force_zero = False
    regression = x_helpers.compute_regressions_all_types(
        df["x"].to_numpy(dtype=float), df["y"].to_numpy(dtype=float),
        force_zero=force_zero,
    )
    regressions_by_group: Dict[str, Any] = {}
    if group_series is not None and "group" in df.columns:
        regressions_by_group = x_helpers.compute_regressions_by_group(
            df, "x", "y", "group", force_zero=force_zero,
        )

    result = {
        "turbine_id": turbine.id,
        "turbine_name": turbine.name,
        "farm_id": turbine.farm.id if turbine.farm else None,
        "farm_name": turbine.farm.name if turbine.farm else None,
        "x_source": x_source,
        "y_source": y_source,
        "group_by": group_by,
        "regression": regression,
        "period": {"start_time_ms": start_ms, "end_time_ms": end_ms},
        "summary": {
            "rows_before_filters": before_count,
            "rows_after_filters": after_count,
            "points_returned": len(points),
        },
        "points": points,
    }
    if regressions_by_group:
        result["regressions_by_group"] = regressions_by_group
    if statistics is not None:
        result["statistics"] = statistics
    return result


class TurbineCrossDataAnalysisAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]

    def post(self, request, turbine_id=None):
        try:
            turbine_id = turbine_id or request.data.get("turbine_id") or request.query_params.get("turbine_id")
            if not turbine_id:
                return error_response("Turbine ID must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            try:
                turbine = Turbines.objects.select_related("farm", "farm__investor").get(id=turbine_id)
            except Turbines.DoesNotExist:
                return error_response("Turbine not found", "TURBINE_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            permission_response = check_object_permission(
                request, self, turbine, "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response

            payload = request.data or {}
            params = _parse_payload(payload)

            if params["x_source"] not in VALID_SOURCES or params["y_source"] not in VALID_SOURCES:
                return error_response(
                    f"x_source and y_source must be in: {', '.join(sorted(VALID_SOURCES))}",
                    "INVALID_PARAMETERS",
                    status.HTTP_400_BAD_REQUEST,
                )
            if params["group_by"] not in VALID_GROUP_BY:
                return error_response(
                    f"group_by must be one of: {', '.join(sorted(VALID_GROUP_BY))}",
                    "INVALID_PARAMETERS",
                    status.HTTP_400_BAD_REQUEST,
                )

            payload_for_cache = {k: v for k, v in (payload or {}).items() if k != "regression"}
            cache_key = x_helpers.get_cross_analysis_cache_key("cross_data", int(turbine.id), payload_for_cache)
            cached = cache.get(cache_key)
            if cached:
                return success_response(cached)

            result = _run_turbine_pipeline(turbine, params)
            if result is None:
                return error_response(
                    "No data available for the specified time range",
                    "NO_DATA",
                    status.HTTP_404_NOT_FOUND,
                )

            cache.set(cache_key, result, timeout=CROSS_ANALYSIS_CACHE_TIMEOUT_SECONDS)
            return success_response(result)

        except Exception as e:
            logger.error("Error in TurbineCrossDataAnalysisAPIView.post: %s", str(e), exc_info=True)
            return error_response(
                "An unexpected error occurred",
                "INTERNAL_SERVER_ERROR",
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
