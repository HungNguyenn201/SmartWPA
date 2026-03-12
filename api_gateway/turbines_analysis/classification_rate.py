import logging
from typing import Any, Dict, List

import pandas as pd
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Turbines
from analytics.models import Computation, ClassificationPoint
from permissions.views import CanViewTurbine
from api_gateway.management.acquisition.helpers import check_object_permission
from api_gateway.turbines_analysis.helpers.response_schema import success_response, error_response
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms

logger = logging.getLogger('api_gateway.turbines_analysis')


class ClassificationRateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def get(self, request, turbine_id=None):
        try:
            if not turbine_id:
                turbine_id = request.query_params.get('turbine_id')
            
            if not turbine_id:
                return error_response("Turbine ID must be specified", "MISSING_PARAMETERS", status.HTTP_400_BAD_REQUEST)
            
            try:
                turbine = Turbines.objects.select_related('farm', 'farm__investor').get(id=turbine_id)
            except Turbines.DoesNotExist:
                return error_response("Turbine not found", "TURBINE_NOT_FOUND", status.HTTP_404_NOT_FOUND)
            
            permission_response = check_object_permission(
                request, self, turbine,
                "You don't have permission to access this turbine"
            )
            if permission_response:
                return permission_response
            
            start_time = request.query_params.get('start_time')
            end_time = request.query_params.get('end_time')
            
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
                    return error_response("start_time and end_time must be integers", "INVALID_PARAMETERS", status.HTTP_400_BAD_REQUEST)
                
                computation = computation_query.filter(
                    start_time=start_time,
                    end_time=end_time
                ).first()
            else:
                computation = computation_query.order_by('-end_time').first()
            
            if not computation:
                logger.warning(f"No classification computation found for turbine {turbine_id}")
                return error_response("No classification found for this turbine", "NO_CLASSIFICATION", status.HTTP_404_NOT_FOUND)
            
            classification_summary = computation.classification_summary.all().order_by('status_code')
            
            classification_rates = {}
            classification_map = {}
            for data in classification_summary:
                classification_rates[str(data.status_code)] = data.percentage
                classification_map[str(data.status_code)] = data.status_name
            
            result = {
                "turbine_id": turbine.id,
                "turbine_name": turbine.name,
                "start_time": to_epoch_ms(computation.start_time) if computation.start_time else None,
                "end_time": to_epoch_ms(computation.end_time) if computation.end_time else None,
                "farm_name": turbine.farm.name if turbine.farm else None,
                "classification_rates": classification_rates,
                "classification_map": classification_map,
            }
            
            return success_response(result)
            
        except Exception as e:
            logger.error(f"Error in ClassificationRateAPIView.get for turbine {turbine_id}: {str(e)}", exc_info=True)
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)


def _month_start_ms_from_timestamp_ms(ts_ms: pd.Series) -> pd.Series:
    """Return month-start timestamps in integer milliseconds (UTC)."""
    dt = pd.to_datetime(ts_ms, unit="ms", utc=True, errors="coerce")
    month_start = dt.dt.to_period("M").dt.start_time
    # to_period().start_time returns tz-naive; normalize to UTC for ms conversion.
    month_start = month_start.dt.tz_localize("UTC")
    return (month_start.view("int64") // 1_000_000).astype("int64")


class MonthlyClassificationRateAPIView(APIView):
    """
    Monthly classification rates for a turbine (manual 1.3.6.2.1 Monthly classification).

    Query params:
      - start_time: epoch ms (optional); if omitted, use computation's start_time
      - end_time: epoch ms (optional); if omitted, use computation's end_time
      - include_errors: '1' to include MEASUREMENT_ERROR in denominator; default excludes it
    """

    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]

    def get(self, request, turbine_id=None):
        try:
            if not turbine_id:
                turbine_id = request.query_params.get("turbine_id")

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

            start_time = request.query_params.get("start_time")
            end_time = request.query_params.get("end_time")

            include_errors = (request.query_params.get("include_errors") or "").strip() == "1"

            computation_query = (
                Computation.objects.filter(turbine=turbine, computation_type="classification", is_latest=True)
                .select_related("turbine", "farm")
                .prefetch_related("classification_summary")
            )

            if start_time is not None and end_time is not None:
                try:
                    start_time_ms = int(start_time)
                    end_time_ms = int(end_time)
                except ValueError:
                    return error_response(
                        "start_time and end_time must be integers (epoch ms)",
                        "INVALID_PARAMETERS",
                        status.HTTP_400_BAD_REQUEST,
                    )
                computation = computation_query.filter(start_time=start_time_ms, end_time=end_time_ms).first()
                if not computation:
                    computation = computation_query.order_by("-end_time").first()
            else:
                computation = computation_query.order_by("-end_time").first()
                if not computation:
                    pass  # will return 404 below
                else:
                    start_time_ms = int(computation.start_time) if computation.start_time is not None else None
                    end_time_ms = int(computation.end_time) if computation.end_time is not None else None

            if not computation:
                logger.warning(f"No classification computation found for turbine {turbine_id}")
                return error_response("No classification found for this turbine", "NO_CLASSIFICATION", status.HTTP_404_NOT_FOUND)

            if start_time_ms is None and computation.start_time is not None:
                start_time_ms = int(computation.start_time)
            if end_time_ms is None and computation.end_time is not None:
                end_time_ms = int(computation.end_time)

            classification_summary = list(computation.classification_summary.all().order_by("status_code"))
            status_name_by_code: Dict[int, str] = {int(r.status_code): str(r.status_name) for r in classification_summary}
            status_code_by_name: Dict[str, int] = {str(r.status_name): int(r.status_code) for r in classification_summary}

            points_qs = ClassificationPoint.objects.filter(computation=computation).only("timestamp", "classification")
            if start_time_ms is not None:
                points_qs = points_qs.filter(timestamp__gte=start_time_ms)
            if end_time_ms is not None:
                points_qs = points_qs.filter(timestamp__lte=end_time_ms)

            rows = list(points_qs.values_list("timestamp", "classification"))
            if not rows:
                return success_response(
                    {
                        "turbine_id": turbine.id,
                        "turbine_name": turbine.name,
                        "farm_name": turbine.farm.name if turbine.farm else None,
                        "start_time": start_time_ms,
                        "end_time": end_time_ms,
                        "classification_map": {str(k): v for k, v in status_name_by_code.items()},
                        "include_errors": include_errors,
                        "monthly_classification": [],
                    }
                )

            df = pd.DataFrame(rows, columns=["timestamp_ms", "status_code"])
            df["timestamp_ms"] = pd.to_numeric(df["timestamp_ms"], errors="coerce")
            df["status_code"] = pd.to_numeric(df["status_code"], errors="coerce")
            df = df.dropna(subset=["timestamp_ms", "status_code"])
            df["status_code"] = df["status_code"].astype("int64")
            df["month_start_ms"] = _month_start_ms_from_timestamp_ms(df["timestamp_ms"])

            if not include_errors:
                err_code = status_code_by_name.get("MEASUREMENT_ERROR")
                if err_code is not None:
                    df = df[df["status_code"] != int(err_code)]

            if df.empty:
                return success_response(
                    {
                        "turbine_id": turbine.id,
                        "turbine_name": turbine.name,
                        "farm_name": turbine.farm.name if turbine.farm else None,
                        "start_time": start_time_ms,
                        "end_time": end_time_ms,
                        "classification_map": {str(k): v for k, v in status_name_by_code.items()},
                        "include_errors": include_errors,
                        "monthly_classification": [],
                    }
                )

            grouped = (
                df.groupby(["month_start_ms", "status_code"], as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )
            monthly_total = grouped.groupby("month_start_ms")["count"].sum().to_dict()

            normal_code = status_code_by_name.get("NORMAL")
            stop_code = status_code_by_name.get("STOP")

            out: List[Dict[str, Any]] = []
            for month_ms, sub in grouped.groupby("month_start_ms"):
                month_ms_int = int(month_ms)
                total = int(monthly_total.get(month_ms_int) or 0)
                counts_by_code = {str(int(r["status_code"])): int(r["count"]) for _, r in sub.iterrows()}
                rates_by_code = {
                    code: ((cnt / total * 100.0) if total > 0 else 0.0) for code, cnt in counts_by_code.items()
                }

                normal_cnt = int(counts_by_code.get(str(normal_code), 0)) if normal_code is not None else 0
                stop_cnt = int(counts_by_code.get(str(stop_code), 0)) if stop_code is not None else 0
                others_cnt = max(0, total - normal_cnt - stop_cnt)

                out.append(
                    {
                        "month_start_ms": month_ms_int,
                        "total_points": total,
                        "counts_by_status_code": counts_by_code,
                        "rates_by_status_code": rates_by_code,
                        "groups": {
                            "Normal": {
                                "count": normal_cnt,
                                "percentage": (normal_cnt / total * 100.0) if total > 0 else 0.0,
                            },
                            "Stop": {
                                "count": stop_cnt,
                                "percentage": (stop_cnt / total * 100.0) if total > 0 else 0.0,
                            },
                            "Others": {
                                "count": others_cnt,
                                "percentage": (others_cnt / total * 100.0) if total > 0 else 0.0,
                            },
                        },
                    }
                )

            out.sort(key=lambda r: r["month_start_ms"])

            return success_response(
                {
                    "turbine_id": turbine.id,
                    "turbine_name": turbine.name,
                    "farm_name": turbine.farm.name if turbine.farm else None,
                    "start_time": start_time_ms,
                    "end_time": end_time_ms,
                    "computation_start_time": to_epoch_ms(computation.start_time) if computation.start_time else None,
                    "computation_end_time": to_epoch_ms(computation.end_time) if computation.end_time else None,
                    "classification_map": {str(k): v for k, v in status_name_by_code.items()},
                    "include_errors": include_errors,
                    "monthly_classification": out,
                }
            )

        except Exception as e:
            logger.error(
                f"Error in MonthlyClassificationRateAPIView.get for turbine {turbine_id}: {str(e)}",
                exc_info=True,
            )
            return error_response("An unexpected error occurred", "INTERNAL_SERVER_ERROR", status.HTTP_500_INTERNAL_SERVER_ERROR)
