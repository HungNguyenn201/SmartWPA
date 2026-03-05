import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from django.core.cache import cache
from django.db.models import Sum
from django.db.models.functions import TruncMonth
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication

from analytics.models import Computation, DailyProduction
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers._header import (
    MONTHLY_DASHBOARD_CACHE_TIMEOUT_SECONDS,
    MONTHLY_DASHBOARD_VARIATION_DEFAULT,
    MONTHLY_DASHBOARD_VARIATION_MAX,
    MONTHLY_DASHBOARD_VARIATION_MIN,
    to_epoch_ms,
)
from api_gateway.turbines_analysis.helpers.farm_dashboard_helpers import (
    aggregate_values,
    get_indicator_value,
    indicator_agg_mode,
    month_start_ms_from_datetime,
    month_start_ms_from_ms,
    parse_indicator_keys,
)
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers.timeseries_helpers import load_timeseries_data
from api_gateway.turbines_analysis.helpers.working_period_helpers import calculate_performance
from facilities.models import Farm, Turbines
from permissions.views import CanViewFarm, CanViewTurbine

logger = logging.getLogger("api_gateway.turbines_analysis")


def _parse_int(value: Optional[str], field: str) -> Tuple[Optional[int], Optional[Response]]:
    if value is None or value == "":
        return None, None
    try:
        return int(value), None
    except ValueError:
        return None, error_response(f"{field} must be an integer", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)


def _pick_latest_computations(
    comps_qs,
    *,
    start_time: Optional[int],
    end_time: Optional[int],
) -> List[Computation]:
    """
    Pick computations for monthly dashboard.

    - If no range: pick latest computation per turbine (qs is expected to be filtered by is_latest=True).
    - If range: pick latest computation per (turbine, month_bucket) within range.
    """
    selected_by_key: Dict[Tuple[int, int], Computation] = {}
    selected_latest_by_turbine: Dict[int, Computation] = {}

    for comp in comps_qs:
        tid = int(comp.turbine_id)
        if start_time is None or end_time is None:
            if tid not in selected_latest_by_turbine:
                selected_latest_by_turbine[tid] = comp
            continue
        # Normalize timestamp to milliseconds before calculating month start
        comp_start_ms = to_epoch_ms(comp.start_time) or comp.start_time
        month_ms = month_start_ms_from_ms(int(comp_start_ms))
        k = (tid, month_ms)
        if k not in selected_by_key:
            selected_by_key[k] = comp

    if start_time is None or end_time is None:
        return list(selected_latest_by_turbine.values())
    return list(selected_by_key.values())


class FarmDashboardMonthlyAnalysisAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewFarm]

    def get(self, request, farm_id=None):
        try:
            if not farm_id:
                farm_id = request.query_params.get("farm_id")
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

            start_time, resp = _parse_int(request.query_params.get("start_time"), "start_time")
            if resp:
                return resp
            end_time, resp = _parse_int(request.query_params.get("end_time"), "end_time")
            if resp:
                return resp

            variation_str = request.query_params.get("variation", str(MONTHLY_DASHBOARD_VARIATION_DEFAULT))
            try:
                variation = int(variation_str)
            except ValueError:
                variation = MONTHLY_DASHBOARD_VARIATION_DEFAULT
            variation = max(MONTHLY_DASHBOARD_VARIATION_MIN, min(MONTHLY_DASHBOARD_VARIATION_MAX, variation))

            indicator_keys = parse_indicator_keys(request.query_params.getlist("indicators", []))

            cache_key = f"monthly_dashboard_farm_{farm.id}_{start_time}_{end_time}_{variation}"
            if indicator_keys:
                cache_key += "_ind_" + "-".join(indicator_keys)
            cached = cache.get(cache_key)
            if cached:
                return success_response(cached)

            turbines = list(Turbines.objects.filter(farm=farm).select_related("farm"))
            if not turbines:
                return error_response("Farm has no turbines", "NO_TURBINES", status.HTTP_404_NOT_FOUND)

            comps_qs = (
                Computation.objects.filter(turbine__in=turbines, computation_type="indicators")
                .select_related("turbine", "farm")
                .prefetch_related("indicator_data")
                .order_by("-created_at")
            )
            if start_time is not None and end_time is not None:
                comps_qs = comps_qs.filter(start_time__gte=start_time, end_time__lte=end_time)
            else:
                comps_qs = comps_qs.filter(is_latest=True)

            selected_comps = _pick_latest_computations(comps_qs, start_time=start_time, end_time=end_time)
            if not selected_comps:
                return error_response("No computation data found", "NO_RESULT_FOUND", status.HTTP_404_NOT_FOUND)

            turbine_ids_with_comp = {int(c.turbine_id) for c in selected_comps}
            turbines_for_output = [t for t in turbines if int(t.id) in turbine_ids_with_comp]

            # Normalize timestamps from DB to milliseconds
            latest_start_time = min(to_epoch_ms(c.start_time) or c.start_time for c in selected_comps)
            latest_end_time = max(to_epoch_ms(c.end_time) or c.end_time for c in selected_comps)

            # ---- Monthly production (bulk aggregation from DailyProduction) ----
            dp_qs = DailyProduction.objects.filter(computation__in=selected_comps)
            if start_time is not None and end_time is not None:
                start_dt = pd.to_datetime(start_time, unit="ms", utc=True).date()
                end_dt = pd.to_datetime(end_time, unit="ms", utc=True).date()
                dp_qs = dp_qs.filter(date__gte=start_dt, date__lte=end_dt)

            dp_rows = (
                dp_qs.annotate(month=TruncMonth("date"))
                .values("computation__turbine_id", "month")
                .annotate(production=Sum("daily_production"), reachable=Sum("daily_reachable"))
                .order_by("month")
            )

            prod_by_turbine: Dict[int, Dict[int, Dict[str, Optional[float]]]] = {}
            prod_acc: Dict[int, Dict[str, Optional[float]]] = {}
            for r in dp_rows:
                tid = int(r["computation__turbine_id"])
                month_dt = r["month"]
                if month_dt is None:
                    continue
                month_ms = month_start_ms_from_datetime(month_dt)
                real_val = float(r["production"] or 0.0)
                reach_val = float(r["reachable"]) if r["reachable"] is not None else None

                tmap = prod_by_turbine.setdefault(tid, {})
                existing = tmap.get(month_ms, {"production": 0.0, "reachable": None})
                existing["production"] = existing["production"] + real_val
                if reach_val is not None:
                    existing["reachable"] = (existing["reachable"] or 0.0) + reach_val
                tmap[month_ms] = existing

                acc = prod_acc.get(month_ms, {"production": 0.0, "reachable": None})
                acc["production"] = acc["production"] + real_val
                if reach_val is not None:
                    acc["reachable"] = (acc["reachable"] or 0.0) + reach_val
                prod_acc[month_ms] = acc

            monthly_production = []
            for ms in sorted(prod_acc.keys()):
                entry = prod_acc[ms]
                real = entry["production"]
                reachable = entry["reachable"]
                loss = max(0.0, reachable - real) if reachable is not None else None
                monthly_production.append({
                    "month_start_ms": ms,
                    "production": float(real),
                    "reachable": float(reachable) if reachable is not None else None,
                    "loss": float(loss) if loss is not None else None,
                })

            # ---- Monthly indicators (selected) ----
            indicators_by_turbine_month: Dict[int, Dict[int, Dict[str, Optional[float]]]] = {}
            if indicator_keys:
                for comp in selected_comps:
                    ind = comp.indicator_data.first() if hasattr(comp, "indicator_data") else None
                    if not ind:
                        continue
                    tid = int(comp.turbine_id)
                    # Normalize timestamp to milliseconds before calculating month start
                    comp_start_ms = to_epoch_ms(comp.start_time) or comp.start_time
                    month_ms = month_start_ms_from_ms(int(comp_start_ms))
                    rec = indicators_by_turbine_month.setdefault(tid, {}).setdefault(month_ms, {})
                    for key in indicator_keys:
                        if key == "DailyProduction":
                            continue
                        rec[key] = get_indicator_value(ind, key)

            monthly_indicators_series: Dict[str, List[Dict[str, Any]]] = {}
            if indicator_keys:
                months_all = sorted({m for tid in indicators_by_turbine_month for m in indicators_by_turbine_month[tid]})
                for key in indicator_keys:
                    if key == "DailyProduction":
                        monthly_indicators_series[key] = [
                            {"month_start_ms": int(ms), "value": float(entry["production"])}
                            for ms, entry in sorted(prod_acc.items(), key=lambda kv: kv[0])
                        ]
                        continue
                    mode = indicator_agg_mode(key)
                    rows: List[Dict[str, Any]] = []
                    for m in months_all:
                        vals = [indicators_by_turbine_month.get(tid, {}).get(m, {}).get(key) for tid in indicators_by_turbine_month]
                        rows.append({"month_start_ms": int(m), "value": aggregate_values(vals, mode)})
                    monthly_indicators_series[key] = rows

            # ---- Monthly performance (compute-on-the-fly from timeseries) ----
            by_turbine: List[Dict[str, Any]] = []
            perf_acc: Dict[int, float] = {}
            for turbine in turbines_for_output:
                perf_rows: List[Dict[str, Any]] = []
                try:
                    df_ts, _, _, _ = load_timeseries_data(turbine, ["power", "wind_speed"], start_time, end_time)
                    if df_ts is not None and not df_ts.empty:
                        perf_rows = calculate_performance(df_ts, variation=variation)
                except Exception as perf_err:
                    logger.warning(
                        "Monthly performance calc failed for turbine %s: %s", turbine.id, str(perf_err), exc_info=True
                    )

                turbine_perf_normalized: List[Dict[str, Any]] = []
                for r in perf_rows:
                    ms = int(r.get("timestamp", 0))
                    perf_acc[ms] = perf_acc.get(ms, 0.0) + float(r.get("performance") or 0.0)
                    turbine_perf_normalized.append({
                        "month_start_ms": ms,
                        "performance": float(r.get("performance") or 0.0),
                    })

                turbine_month_prod = prod_by_turbine.get(int(turbine.id), {})
                turbine_monthly_prod_list = []
                for ms in sorted(turbine_month_prod.keys()):
                    entry = turbine_month_prod[ms]
                    real = entry["production"]
                    reachable = entry["reachable"]
                    loss = max(0.0, reachable - real) if reachable is not None else None
                    turbine_monthly_prod_list.append({
                        "month_start_ms": ms,
                        "production": float(real),
                        "reachable": float(reachable) if reachable is not None else None,
                        "loss": float(loss) if loss is not None else None,
                    })

                row: Dict[str, Any] = {
                    "turbine_id": turbine.id,
                    "turbine_name": turbine.name,
                    "monthly_production": turbine_monthly_prod_list,
                    "monthly_performance": turbine_perf_normalized,
                }
                if indicator_keys:
                    per_month = indicators_by_turbine_month.get(int(turbine.id), {})
                    months_sorted = sorted(per_month.keys())
                    turbine_monthly_indicators: Dict[str, List[Dict[str, Any]]] = {}
                    for key in indicator_keys:
                        if key == "DailyProduction":
                            turbine_monthly_indicators[key] = [
                                {"month_start_ms": int(ms), "value": float(entry["production"])}
                                for ms, entry in sorted(turbine_month_prod.items(), key=lambda kv: kv[0])
                            ]
                        else:
                            turbine_monthly_indicators[key] = [
                                {"month_start_ms": int(m), "value": per_month.get(m, {}).get(key)}
                                for m in months_sorted
                            ]
                    row["monthly_indicators"] = turbine_monthly_indicators
                by_turbine.append(row)

            monthly_performance = [
                {"month_start_ms": ms, "performance": float(v)}
                for ms, v in sorted(perf_acc.items(), key=lambda kv: kv[0])
            ]

            result: Dict[str, Any] = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": to_epoch_ms(latest_start_time) if latest_start_time is not None else (to_epoch_ms(start_time) if start_time is not None else None),
                "end_time": to_epoch_ms(latest_end_time) if latest_end_time is not None else (to_epoch_ms(end_time) if end_time is not None else None),
                "variation": variation,
                "series": {
                    "monthly_production": monthly_production,
                    "monthly_performance": monthly_performance,
                },
                "table": {"by_turbine": by_turbine},
            }
            if indicator_keys:
                result["selected_indicators"] = indicator_keys
                result["series"]["monthly_indicators"] = monthly_indicators_series

            cache.set(cache_key, result, timeout=MONTHLY_DASHBOARD_CACHE_TIMEOUT_SECONDS)
            return success_response(result)

        except Exception as e:
            logger.error("Error in FarmDashboardMonthlyAnalysisAPIView.get: %s", str(e), exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


class TurbineDashboardMonthlyAnalysisAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]

    def get(self, request, turbine_id=None):
        try:
            turbine_id = turbine_id or request.query_params.get("turbine_id")
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

            start_time, resp = _parse_int(request.query_params.get("start_time"), "start_time")
            if resp:
                return resp
            end_time, resp = _parse_int(request.query_params.get("end_time"), "end_time")
            if resp:
                return resp

            variation_str = request.query_params.get("variation", str(MONTHLY_DASHBOARD_VARIATION_DEFAULT))
            try:
                variation = int(variation_str)
            except ValueError:
                variation = MONTHLY_DASHBOARD_VARIATION_DEFAULT
            variation = max(MONTHLY_DASHBOARD_VARIATION_MIN, min(MONTHLY_DASHBOARD_VARIATION_MAX, variation))

            indicator_keys = parse_indicator_keys(request.query_params.getlist("indicators", []))

            cache_key = f"monthly_dashboard_turbine_{turbine.id}_{start_time}_{end_time}_{variation}"
            if indicator_keys:
                cache_key += "_ind_" + "-".join(indicator_keys)
            cached = cache.get(cache_key)
            if cached:
                return success_response(cached)

            comps_qs = (
                Computation.objects.filter(turbine=turbine, computation_type="indicators")
                .select_related("turbine", "farm")
                .prefetch_related("indicator_data")
                .order_by("-created_at")
            )
            if start_time is not None and end_time is not None:
                comps_qs = comps_qs.filter(start_time__gte=start_time, end_time__lte=end_time)
            else:
                comps_qs = comps_qs.filter(is_latest=True)

            selected_comps = _pick_latest_computations(comps_qs, start_time=start_time, end_time=end_time)
            if not selected_comps:
                return error_response("No computation data found", "NO_RESULT_FOUND", status.HTTP_404_NOT_FOUND)

            # Normalize timestamps from DB to milliseconds
            latest_start_time = min(to_epoch_ms(c.start_time) or c.start_time for c in selected_comps)
            latest_end_time = max(to_epoch_ms(c.end_time) or c.end_time for c in selected_comps)

            # Monthly production for this turbine
            dp_qs = DailyProduction.objects.filter(computation__in=selected_comps)
            if start_time is not None and end_time is not None:
                start_dt = pd.to_datetime(start_time, unit="ms", utc=True).date()
                end_dt = pd.to_datetime(end_time, unit="ms", utc=True).date()
                dp_qs = dp_qs.filter(date__gte=start_dt, date__lte=end_dt)

            dp_rows = (
                dp_qs.annotate(month=TruncMonth("date"))
                .values("month")
                .annotate(production=Sum("daily_production"), reachable=Sum("daily_reachable"))
                .order_by("month")
            )
            prod_map: Dict[int, Dict[str, Optional[float]]] = {}
            for r in dp_rows:
                month_dt = r["month"]
                if month_dt is None:
                    continue
                month_ms = month_start_ms_from_datetime(month_dt)
                real_val = float(r["production"] or 0.0)
                reach_val = float(r["reachable"]) if r["reachable"] is not None else None
                existing = prod_map.get(month_ms, {"production": 0.0, "reachable": None})
                existing["production"] = existing["production"] + real_val
                if reach_val is not None:
                    existing["reachable"] = (existing["reachable"] or 0.0) + reach_val
                prod_map[month_ms] = existing

            monthly_production = []
            for ms in sorted(prod_map.keys()):
                entry = prod_map[ms]
                real = entry["production"]
                reachable = entry["reachable"]
                loss = max(0.0, reachable - real) if reachable is not None else None
                monthly_production.append({
                    "month_start_ms": ms,
                    "production": float(real),
                    "reachable": float(reachable) if reachable is not None else None,
                    "loss": float(loss) if loss is not None else None,
                })

            # Monthly indicators
            monthly_indicators_series: Dict[str, List[Dict[str, Any]]] = {}
            if indicator_keys:
                per_month: Dict[int, Dict[str, Optional[float]]] = {}
                for comp in selected_comps:
                    ind = comp.indicator_data.first() if hasattr(comp, "indicator_data") else None
                    if not ind:
                        continue
                    m = month_start_ms_from_ms(int(comp.start_time))
                    rec = per_month.setdefault(m, {})
                    for key in indicator_keys:
                        if key == "DailyProduction":
                            continue
                        rec[key] = get_indicator_value(ind, key)

                months_sorted = sorted(set(list(per_month.keys()) + list(prod_map.keys())))
                for key in indicator_keys:
                    if key == "DailyProduction":
                        monthly_indicators_series[key] = [
                            {"month_start_ms": int(ms), "value": float(prod_map[ms]["production"])}
                            for ms in sorted(prod_map.keys())
                        ]
                    else:
                        monthly_indicators_series[key] = [
                            {"month_start_ms": int(m), "value": per_month.get(m, {}).get(key)}
                            for m in months_sorted
                        ]

            # Monthly performance
            perf_rows: List[Dict[str, Any]] = []
            try:
                df_ts, _, _, _ = load_timeseries_data(turbine, ["power", "wind_speed"], start_time, end_time)
                if df_ts is not None and not df_ts.empty:
                    perf_rows = calculate_performance(df_ts, variation=variation)
            except Exception as perf_err:
                logger.warning(
                    "Monthly performance calc failed for turbine %s: %s", turbine.id, str(perf_err), exc_info=True
                )

            monthly_performance = [
                {"month_start_ms": int(r.get("timestamp", 0)), "performance": float(r.get("performance") or 0.0)}
                for r in perf_rows
            ]

            result: Dict[str, Any] = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "farm_id": turbine.farm.id if turbine.farm else None,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "start_time": to_epoch_ms(latest_start_time) if latest_start_time is not None else (to_epoch_ms(start_time) if start_time is not None else None),
                "end_time": to_epoch_ms(latest_end_time) if latest_end_time is not None else (to_epoch_ms(end_time) if end_time is not None else None),
                "variation": variation,
                "series": {
                    "monthly_production": monthly_production,
                    "monthly_performance": monthly_performance,
                },
            }
            if indicator_keys:
                result["selected_indicators"] = indicator_keys
                result["series"]["monthly_indicators"] = monthly_indicators_series

            cache.set(cache_key, result, timeout=MONTHLY_DASHBOARD_CACHE_TIMEOUT_SECONDS)
            return success_response(result)

        except Exception as e:
            logger.error("Error in TurbineDashboardMonthlyAnalysisAPIView.get: %s", str(e), exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)

