"""User-specific validators"""
import re
from permissions.models import Account
from api_gateway.management.common.validators import validate_email

def validate_username(username):
    """Validate username format và length"""
    if not username:
        return {"valid": False, "error": "Username cannot be empty", "code": "EMPTY_USERNAME"}
    if len(username) < 4:
        return {"valid": False, "error": "Username must be at least 4 characters long", "code": "INVALID_USERNAME_LENGTH"}
    if len(username) > 150:
        return {"valid": False, "error": "Username is too long (max 150 characters)", "code": "INVALID_USERNAME_LENGTH"}
    if not re.match(r'^[a-zA-Z0-9_]+$', username):
        return {"valid": False, "error": "Username can only contain letters, numbers, and underscores", "code": "INVALID_USERNAME_FORMAT"}
    if Account.objects.filter(username=username).exists():
        return {"valid": False, "error": "Username already exists", "code": "USERNAME_EXISTS"}
    return {"valid": True}

def validate_password(password):
    """Validate password strength"""
    if not password:
        return {"valid": False, "error": "Password cannot be empty", "code": "EMPTY_PASSWORD"}
    if len(password) < 8:
        return {"valid": False, "error": "Password must be at least 8 characters long", "code": "INVALID_PASSWORD_LENGTH"}
    if not re.match(r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]+$', password):
        return {"valid": False, "error": "Password must contain at least one uppercase letter, one lowercase letter, one number and one special character", "code": "INVALID_PASSWORD_FORMAT"}
    return {"valid": True}

def validate_user_input(username=None, email=None, password=None, exclude_user_id=None):
    """Validate tất cả user input"""
    errors = []
    if username is not None:
        result = validate_username(username)
        if not result["valid"]:
            errors.append(result)
    if email is not None:
        result = validate_email(email, exclude_user_id)
        if not result["valid"]:
            errors.append(result)
    if password is not None:
        result = validate_password(password)
        if not result["valid"]:
            errors.append(result)
    return errors

