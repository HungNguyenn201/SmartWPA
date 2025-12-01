"""Turbine-specific validators"""
from facilities.models import Turbines
from api_gateway.management.common.validators import validate_name, validate_capacity, validate_coordinate

def validate_turbine_name(name, farm, exclude_turbine_id=None):
    """Validate turbine name - name must be unique within a farm"""
    if not name:
        return {"valid": False, "error": "Turbine name is required", "code": "EMPTY_NAME"}
    if len(name) < 3:
        return {"valid": False, "error": "Name must be at least 3 characters long", "code": "INVALID_NAME_LENGTH"}
    if len(name) > 100:
        return {"valid": False, "error": "Name is too long (max 100 characters)", "code": "INVALID_NAME_LENGTH"}
    import re
    if not re.match(r'^[a-zA-Z0-9_ ]+$', name):
        return {"valid": False, "error": "Name contains invalid characters", "code": "INVALID_NAME_FORMAT"}
    
    query = Turbines.objects.filter(farm=farm, name__iexact=name)
    if exclude_turbine_id:
        query = query.exclude(id=exclude_turbine_id)
    if query.exists():
        return {"valid": False, "error": "Turbine name already exists in this farm", "code": "NAME_EXISTS"}
    
    return {"valid": True}

def validate_turbine_data(name=None, capacity=None, latitude=None, longitude=None, farm=None, exclude_turbine_id=None):
    """Validate tất cả turbine data"""
    errors = []
    if name is not None and farm is not None:
        result = validate_turbine_name(name, farm, exclude_turbine_id)
        if not result["valid"]:
            errors.append(result)
    if capacity is not None:
        result = validate_capacity(capacity, max_value=10000)  # Turbine có capacity lớn hơn farm
        if not result["valid"]:
            errors.append(result)
    if latitude is not None:
        result = validate_coordinate(latitude, "latitude")
        if not result["valid"]:
            errors.append(result)
    if longitude is not None:
        result = validate_coordinate(longitude, "longitude")
        if not result["valid"]:
            errors.append(result)
    return errors

