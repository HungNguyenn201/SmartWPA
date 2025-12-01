"""Common helper functions and exceptions"""
from facilities.models import Investor
import logging

logger = logging.getLogger(__name__)

# Custom exceptions
class UserValidationError(Exception):
    """Exception for user validation errors"""
    pass

class FarmValidationError(Exception):
    """Exception for farm validation errors"""
    pass

class TurbineValidationError(Exception):
    """Exception for turbine validation errors"""
    pass

# Helper functions
def create_or_get_investor(email, username, is_active=True):
    """Tạo hoặc lấy Investor object và đảm bảo có License"""
    try:
        investor_obj = Investor.objects.get(email=email)
        investor_obj.generate_license()
    except Investor.DoesNotExist:
        investor_obj = Investor.objects.create(
            name=username,
            email=email,
            is_active=is_active
        )
        investor_obj.generate_license()
    return investor_obj

