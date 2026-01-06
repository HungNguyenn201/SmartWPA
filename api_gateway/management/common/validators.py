"""Common validators"""
import re
from permissions.models import Account

def validate_email(email, exclude_user_id=None):
    """Validate email format và uniqueness"""
    if not email:
        return {"valid": False, "error": "Email cannot be empty", "code": "EMPTY_EMAIL"}
    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        return {"valid": False, "error": "Invalid email format", "code": "INVALID_EMAIL"}
    query = Account.objects.filter(email=email)
    if exclude_user_id:
        query = query.exclude(id=exclude_user_id)
    if query.exists():
        return {"valid": False, "error": "Email already exists", "code": "EMAIL_EXISTS"}
    return {"valid": True}

def validate_name(name, min_length=3, max_length=100, model_class=None, exclude_id=None, field_name="name", db_field_name="name"):
    """Generic name validator"""
    if not name:
        return {"valid": False, "error": f"{field_name} is required", "code": "EMPTY_NAME"}
    if len(name) < min_length:
        return {"valid": False, "error": f"{field_name} must be at least {min_length} characters long", "code": "INVALID_NAME_LENGTH"}
    if len(name) > max_length:
        return {"valid": False, "error": f"{field_name} is too long (max {max_length} characters)", "code": "INVALID_NAME_LENGTH"}
    if not re.match(r'^[a-zA-Z0-9_ ]+$', name):
        return {"valid": False, "error": f"{field_name} contains invalid characters", "code": "INVALID_NAME_FORMAT"}
    
    if model_class:
        query = model_class.objects.filter(**{f"{db_field_name}__iexact": name})
        if exclude_id:
            query = query.exclude(id=exclude_id)
        if query.exists():
            return {"valid": False, "error": f"{field_name} already exists", "code": "NAME_EXISTS"}
    
    return {"valid": True}

def validate_capacity(capacity, max_value=1000):
    """Validate capacity"""
    if capacity is None:
        return {"valid": True}
    try:
        capacity = float(capacity)
        if capacity <= 0:
            return {"valid": False, "error": "Capacity must be a positive number", "code": "INVALID_CAPACITY"}
        if capacity > max_value:
            return {"valid": False, "error": f"Capacity exceeds maximum allowed value ({max_value} MW)", "code": "CAPACITY_EXCEEDED"}
    except (TypeError, ValueError):
        return {"valid": False, "error": "Invalid capacity value", "code": "INVALID_CAPACITY"}
    return {"valid": True}

def validate_coordinate(value, coord_type="latitude"):
    """Validate latitude or longitude"""
    if value is None:
        return {"valid": True}
    try:
        value = float(value)
        if coord_type == "latitude":
            if value < -90 or value > 90:
                return {"valid": False, "error": "Latitude must be between -90 and 90", "code": "INVALID_LATITUDE"}
        elif coord_type == "longitude":
            if value < -180 or value > 180:
                return {"valid": False, "error": "Longitude must be between -180 and 180", "code": "INVALID_LONGITUDE"}
    except (TypeError, ValueError):
        return {"valid": False, "error": f"Invalid {coord_type} value", "code": f"INVALID_{coord_type.upper()}"}
    return {"valid": True}

