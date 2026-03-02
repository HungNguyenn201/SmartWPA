"""
Chuẩn hoá response schema cho turbines_analysis APIs.

Quy ước:
- Thành công: { "success": true, "data": ... }
- Lỗi: { "success": false, "error": str, "code": str }
- API luôn trả HTTP status tương ứng (2xx / 4xx / 5xx).
"""
from rest_framework.response import Response


def success_response(data, status=200, message=None):
    """Trả response thành công thống nhất. message (optional) thêm vào body nếu cần."""
    body = {"success": True, "data": data}
    if message is not None:
        body["message"] = message
    return Response(body, status=status)


def error_response(error: str, code: str, status=400):
    """Trả response lỗi thống nhất."""
    return Response(
        {"success": False, "error": error, "code": code},
        status=status,
    )
