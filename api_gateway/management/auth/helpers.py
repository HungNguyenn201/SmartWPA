"""Authentication helper functions"""
from rest_framework_simplejwt.tokens import RefreshToken
from permissions.models import Account, License
import logging

logger = logging.getLogger(__name__)

def get_token_for_user(user):
    """Tạo JWT token cho user"""
    refresh = RefreshToken.for_user(user)
    return {
        'access': str(refresh.access_token),
        'refresh': str(refresh)
    }

def check_license(user):
    """
    Kiểm tra license của user, với các trường hợp:
    - Nếu là investor, kiểm tra license của investor_profile
    - Nếu là farm_admin hoặc staff, kiểm tra license của farm.investor
    """
    try:
        if user.role == 'investor':
            if not user.investor_profile:
                logger.warning(f"No investor profile found for user {user.username}")
                return False
            license_obj = License.objects.get(investor=user.investor_profile)
            return license_obj.is_valid()
        elif user.role in ['farm_admin', 'staff'] and user.farm and user.farm.investor:
            license_obj = License.objects.get(investor=user.farm.investor)
            return license_obj.is_valid()
        logger.warning(f"Invalid role or missing data for license check: {user.role}")
        return False
    except License.DoesNotExist:
        logger.warning(f"License not found for user {user.username}")
        return False
    except Exception as e:
        logger.error(f"Error checking license for user {user.username}: {str(e)}")
        return False

