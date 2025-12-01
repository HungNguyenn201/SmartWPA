"""Acquisition-specific validators"""
import re
from acquisition.models import SmartHIS, PointType, HISPoint
from api_gateway.management.common.validators import validate_name

def validate_url(url):
    """Validate URL format"""
    if not url:
        return {"valid": False, "error": "URL is required", "code": "EMPTY_URL"}
    if not re.match(r'^https?://', url):
        return {"valid": False, "error": "URL must start with http:// or https://", "code": "INVALID_URL_FORMAT"}
    if len(url) > 200:
        return {"valid": False, "error": "URL is too long (max 200 characters)", "code": "INVALID_URL_LENGTH"}
    return {"valid": True}

def validate_username(username):
    """Validate username for SmartHIS"""
    if not username:
        return {"valid": False, "error": "Username is required", "code": "EMPTY_USERNAME"}
    if len(username) < 3:
        return {"valid": False, "error": "Username must be at least 3 characters long", "code": "INVALID_USERNAME_LENGTH"}
    if len(username) > 20:
        return {"valid": False, "error": "Username is too long (max 20 characters)", "code": "INVALID_USERNAME_LENGTH"}
    if not re.match(r'^[a-zA-Z0-9_]+$', username):
        return {"valid": False, "error": "Username can only contain letters, numbers, and underscores", "code": "INVALID_USERNAME_FORMAT"}
    return {"valid": True}

def validate_password(password):
    """Validate password for SmartHIS"""
    if not password:
        return {"valid": False, "error": "Password is required", "code": "EMPTY_PASSWORD"}
    if len(password) < 3:
        return {"valid": False, "error": "Password must be at least 3 characters long", "code": "INVALID_PASSWORD_LENGTH"}
    if len(password) > 20:
        return {"valid": False, "error": "Password is too long (max 20 characters)", "code": "INVALID_PASSWORD_LENGTH"}
    return {"valid": True}

def validate_smart_his_data(address=None, username=None, password=None, farm=None, exclude_smart_his_id=None):
    """Validate SmartHIS data"""
    errors = []
    if address is not None:
        result = validate_url(address)
        if not result["valid"]:
            errors.append(result)
    if username is not None:
        result = validate_username(username)
        if not result["valid"]:
            errors.append(result)
    if password is not None:
        result = validate_password(password)
        if not result["valid"]:
            errors.append(result)
    if farm is not None:
        # Kiểm tra xem farm đã có SmartHIS chưa (nếu đang tạo mới)
        if exclude_smart_his_id is None:
            if SmartHIS.objects.filter(farm=farm).exists():
                errors.append({"valid": False, "error": "This farm already has a SmartHIS configuration", "code": "SMARTHIS_EXISTS"})
    return errors

def validate_his_point_data(point_name=None, farm=None, point_type=None, turbine=None, exclude_his_point_id=None):
    """Validate HISPoint data"""
    errors = []
    if point_name is not None:
        if not point_name:
            errors.append({"valid": False, "error": "Point name is required", "code": "EMPTY_POINT_NAME"})
        if len(point_name) > 200:
            errors.append({"valid": False, "error": "Point name is too long (max 200 characters)", "code": "INVALID_POINT_NAME_LENGTH"})
    
    if point_type is not None and farm is not None:
        # Kiểm tra unique_together constraint
        query = HISPoint.objects.filter(farm=farm, point_type=point_type, turbine=turbine)
        if exclude_his_point_id:
            query = query.exclude(id=exclude_his_point_id)
        if query.exists():
            errors.append({"valid": False, "error": "HISPoint with this farm, point_type, and turbine already exists", "code": "HISPOINT_EXISTS"})
        
        # Kiểm tra logic: nếu point_type.level là 'turbine' thì phải có turbine
        if point_type.level == 'turbine' and not turbine:
            errors.append({"valid": False, "error": "Turbine level point type requires a turbine", "code": "TURBINE_REQUIRED"})
        
        # Kiểm tra logic: nếu point_type.level là 'farm' thì không nên có turbine
        if point_type.level == 'farm' and turbine:
            errors.append({"valid": False, "error": "Farm level point type should not have a turbine", "code": "TURBINE_NOT_ALLOWED"})
    
    return errors

