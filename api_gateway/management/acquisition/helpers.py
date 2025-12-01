"""Acquisition-specific helper functions"""
from rest_framework.response import Response
from rest_framework import status
import logging

logger = logging.getLogger(__name__)

def check_object_permission(request, view, obj, error_message="You don't have permission"):
    """Helper function để kiểm tra object permission"""
    for permission in view.get_permissions():
        if hasattr(permission, 'has_object_permission'):
            if not permission.has_object_permission(request, view, obj):
                return Response({
                    "success": False,
                    "error": error_message,
                    "code": "ACCESS_DENIED"
                }, status=status.HTTP_403_FORBIDDEN)
    return None

def get_smart_his_serialized_data(smart_his):
    """Serialize SmartHIS data for response"""
    return {
        "id": smart_his.id,
        "farm": {
            "id": smart_his.farm.id,
            "name": smart_his.farm.name
        } if smart_his.farm else None,
        "address": smart_his.address,
        "username": smart_his.username,
        "point_check_expired": smart_his.point_check_expired,
        "created_at": smart_his.created_at.isoformat() if smart_his.created_at else None
    }

def get_point_type_serialized_data(point_type):
    """Serialize PointType data for response"""
    return {
        "id": point_type.id,
        "key": point_type.key,
        "name": point_type.name,
        "level": point_type.level,
        "column_name": point_type.column_name
    }

def get_his_point_serialized_data(his_point):
    """Serialize HISPoint data for response"""
    return {
        "id": his_point.id,
        "point_name": his_point.point_name,
        "farm": {
            "id": his_point.farm.id,
            "name": his_point.farm.name
        } if his_point.farm else None,
        "point_type": {
            "id": his_point.point_type.id,
            "key": his_point.point_type.key,
            "name": his_point.point_type.name,
            "level": his_point.point_type.level
        } if his_point.point_type else None,
        "turbine": {
            "id": his_point.turbine.id,
            "name": his_point.turbine.name
        } if his_point.turbine else None,
        "is_active": his_point.is_active,
        "created_at": his_point.created_at.isoformat() if his_point.created_at else None,
        "updated_at": his_point.updated_at.isoformat() if his_point.updated_at else None
    }
