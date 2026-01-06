"""Farm-specific validators"""
from facilities.models import Farm
from api_gateway.management.common.validators import validate_name, validate_capacity, validate_coordinate

def validate_farm_name(name, exclude_farm_id=None):
    """Validate farm name"""
    return validate_name(name, min_length=3, max_length=100, model_class=Farm, exclude_id=exclude_farm_id, field_name="Farm name", db_field_name="name")

def validate_farm_data(name=None, capacity=None, latitude=None, longitude=None, exclude_farm_id=None):
    """Validate tất cả farm data"""
    errors = []
    if name is not None:
        result = validate_farm_name(name, exclude_farm_id)
        if not result["valid"]:
            errors.append(result)
    if capacity is not None:
        result = validate_capacity(capacity)
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

