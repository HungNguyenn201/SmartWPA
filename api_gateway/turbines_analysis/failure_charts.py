import logging
from typing import Any, Dict, List, Optional, Tuple

from datetime import datetime, timezone

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication

from analytics.models import Computation, FailureEvent, IndicatorData
from facilities.models import Farm, Turbines
from permissions.views import CanViewFarm
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms

logger = logging.getLogger("api_gateway.turbines_analysis")


def _parse_int(value: Optional[str], field: str) -> Tuple[Optional[int], Optional[Response]]:
    if value is None or value == "":
        return None, None
    try:
        return int(value), None
    except ValueError:
        return None, error_response(f"{field} must be an integer", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)


def _seconds_to_days(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(x) / 86400.0


def _month_start_ms_list(start_ms: int, end_ms: int) -> List[int]:
    """
    Build month-start timestamps (ms) in UTC covering [start_ms, end_ms] (inclusive).
    FE can format these ticks as J, F, M... and still disambiguate by year.
    
    Args:
        start_ms: Start timestamp (will be normalized to milliseconds)
        end_ms: End timestamp (will be normalized to milliseconds)
    """
    if end_ms < start_ms:
        return []

    # Normalize timestamps to milliseconds (handle legacy data in seconds)
    start_ms = to_epoch_ms(start_ms) or start_ms
    end_ms = to_epoch_ms(end_ms) or end_ms

    start_dt = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    cur = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
    end_month = datetime(end_dt.year, end_dt.month, 1, tzinfo=timezone.utc)

    out: List[int] = []
    while cur <= end_month:
        out.append(int(cur.timestamp() * 1000))
        # increment month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            cur = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)
    return out


class FarmFailureIndicatorsChartAPIView(APIView):
    """
    API for the 'Mean number of failure' / histogram chart.

    Data source:
    - Read from persisted IndicatorData (computed during computation, saved to DB).

    Time filtering:
    - If start_time & end_time are provided: use the Computation with exact range (per turbine).
    - Else: use the latest computation (per turbine).
    """

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

            turbines = Turbines.objects.filter(farm=farm).select_related("farm")
            if not turbines.exists():
                return error_response("No turbines in this farm", "NO_TURBINES", status.HTTP_404_NOT_FOUND)

            comp_q = Computation.objects.filter(turbine__in=turbines, computation_type="indicators").select_related(
                "turbine", "farm"
            ).prefetch_related("indicator_data")

            if start_time is not None and end_time is not None:
                comp_q = comp_q.filter(start_time=start_time, end_time=end_time).order_by("-created_at")
            else:
                comp_q = comp_q.filter(is_latest=True).order_by("-end_time", "-created_at")

            # Pick one computation per turbine
            by_turbine: Dict[int, Computation] = {}
            for comp in comp_q:
                tid = comp.turbine_id
                if tid not in by_turbine:
                    by_turbine[tid] = comp

            indicators = {"FailureCount": [], "Mttr": [], "Mttf": [], "Mtbf": []}
            turbines_out: List[Dict[str, Any]] = []

            latest_start_time: Optional[int] = None
            latest_end_time: Optional[int] = None

            for turbine in turbines:
                comp = by_turbine.get(turbine.id)
                if not comp:
                    continue

                ind: Optional[IndicatorData] = comp.indicator_data.first() if hasattr(comp, "indicator_data") else None
                if not ind:
                    continue

                # Normalize timestamps from DB to milliseconds
                comp_start_ms = to_epoch_ms(comp.start_time) or comp.start_time
                comp_end_ms = to_epoch_ms(comp.end_time) or comp.end_time
                
                if latest_start_time is None or comp_start_ms < latest_start_time:
                    latest_start_time = comp_start_ms
                if latest_end_time is None or comp_end_ms > latest_end_time:
                    latest_end_time = comp_end_ms

                turbines_out.append({"turbine_id": turbine.id, "turbine_name": turbine.name})

                indicators["FailureCount"].append(
                    {"turbine_id": turbine.id, "turbine_name": turbine.name, "value": int(ind.failure_count or 0)}
                )
                indicators["Mttr"].append(
                    {"turbine_id": turbine.id, "turbine_name": turbine.name, "value": _seconds_to_days(ind.mttr)}
                )
                indicators["Mttf"].append(
                    {"turbine_id": turbine.id, "turbine_name": turbine.name, "value": _seconds_to_days(ind.mttf)}
                )
                indicators["Mtbf"].append(
                    {"turbine_id": turbine.id, "turbine_name": turbine.name, "value": _seconds_to_days(ind.mtbf)}
                )

            if not turbines_out:
                return error_response("No indicators data found", "NO_RESULT_FOUND", status.HTTP_404_NOT_FOUND)

            # Normalize timestamps to milliseconds before returning
            normalized_start_time = to_epoch_ms(latest_start_time) if latest_start_time is not None else None
            normalized_end_time = to_epoch_ms(latest_end_time) if latest_end_time is not None else None
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": normalized_start_time,
                "end_time": normalized_end_time,
                "turbines": turbines_out,
                "indicators": indicators,
                "unit": {"Mttr": "days", "Mttf": "days", "Mtbf": "days"},
            }
            return success_response(result)

        except Exception as e:
            logger.error("Error in FarmFailureIndicatorsChartAPIView.get: %s", str(e), exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


class FarmFailureTimelineChartAPIView(APIView):
    """
    API for the 'Turbine Failure Chart (Timeline)'.

    Data source:
    - Read from persisted FailureEvent rows (computed during computation, saved to DB).

    Time filtering:
    - If start_time & end_time are provided: use the classification Computation with exact range (per turbine).
    - Else: use the latest classification computation (per turbine).
    """

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

            turbines = Turbines.objects.filter(farm=farm).select_related("farm")
            if not turbines.exists():
                return error_response("No turbines in this farm", "NO_TURBINES", status.HTTP_404_NOT_FOUND)

            comp_q = Computation.objects.filter(turbine__in=turbines, computation_type="classification").select_related(
                "turbine", "farm"
            )

            if start_time is not None and end_time is not None:
                comp_q = comp_q.filter(start_time=start_time, end_time=end_time).order_by("-created_at")
            else:
                comp_q = comp_q.filter(is_latest=True).order_by("-end_time", "-created_at")

            by_turbine: Dict[int, Computation] = {}
            for comp in comp_q:
                tid = comp.turbine_id
                if tid not in by_turbine:
                    by_turbine[tid] = comp

            # Bulk load events for selected computations
            comps = list(by_turbine.values())
            events_q = FailureEvent.objects.filter(computation__in=comps).order_by("start_time")
            events_by_comp: Dict[int, List[FailureEvent]] = {}
            for ev in events_q:
                events_by_comp.setdefault(ev.computation_id, []).append(ev)

            turbines_out: List[Dict[str, Any]] = []
            latest_start_time: Optional[int] = None
            latest_end_time: Optional[int] = None

            for turbine in turbines:
                comp = by_turbine.get(turbine.id)
                if not comp:
                    continue

                # Normalize timestamps from DB to milliseconds
                comp_start_ms = to_epoch_ms(comp.start_time) or comp.start_time
                comp_end_ms = to_epoch_ms(comp.end_time) or comp.end_time
                
                if latest_start_time is None or comp_start_ms < latest_start_time:
                    latest_start_time = comp_start_ms
                if latest_end_time is None or comp_end_ms > latest_end_time:
                    latest_end_time = comp_end_ms

                evs = events_by_comp.get(comp.id, [])
                # Clip events to requested/selected range so the X-axis doesn't get distorted by out-of-range rows.
                clip_start = to_epoch_ms(start_time) if start_time is not None else comp_start_ms
                clip_end = to_epoch_ms(end_time) if end_time is not None else comp_end_ms

                clipped_events: List[Dict[str, Any]] = []
                for e in evs:
                    s = int(e.start_time)
                    en = int(e.end_time)
                    if en <= clip_start or s >= clip_end:
                        continue
                    s2 = max(s, clip_start)
                    en2 = min(en, clip_end)
                    clipped_events.append(
                        {
                            "start_time": int(s2),
                            "end_time": int(en2),
                            "duration_s": float((en2 - s2) / 1000.0),
                            "status": e.status,
                        }
                    )

                turbines_out.append(
                    {
                        "turbine_id": turbine.id,
                        "turbine_name": turbine.name,
                        "events": clipped_events,
                    }
                )

            if not turbines_out:
                return error_response("No classification data found", "NO_RESULT_FOUND", status.HTTP_404_NOT_FOUND)

            # Normalize timestamps to milliseconds before returning
            normalized_start_time = to_epoch_ms(latest_start_time) if latest_start_time is not None else None
            normalized_end_time = to_epoch_ms(latest_end_time) if latest_end_time is not None else None
            
            result = {
                "farm_id": farm.id,
                "farm_name": farm.name,
                "start_time": normalized_start_time,
                "end_time": normalized_end_time,
                # Explicit month ticks (month-start ms) for the X-axis (J,F,M... across years)
                "months": _month_start_ms_list(int(normalized_start_time), int(normalized_end_time))
                if normalized_start_time is not None and normalized_end_time is not None
                else [],
                "turbines": turbines_out,
            }
            return success_response(result)

        except Exception as e:
            logger.error("Error in FarmFailureTimelineChartAPIView.get: %s", str(e), exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)

