# Management module exports
# Import all views for backward compatibility

# Auth views
from .auth import UserLoginView, LogoutAPIView, TokenRefreshView

# License views
from .license import LicenseManagementView

# User views
from .users import (
    AdminCreateUserAPIView,
    InvestorCreateUserAPIView,
    FarmAdminCreateUserAPIView,
    UserInfoView,
    UserListAPIView,
    UserUpdateAPIView,
    UserDeleteAPIView
)

# Farm views
from .farms import (
    FarmCreateAPIView,
    FarmUpdateAPIView,
    FarmDeleteAPIView,
    FarmListAPIView,
    FarmDetailsView
)

# Turbine views
from .turbines import (
    TurbineCreateAPIView,
    TurbineUpdateAPIView,
    TurbineDeleteAPIView,
    TurbineListAPIView,
    TurbineDetailsView
)

# Acquisition views
from .acquisition import (
    SmartHISCreateAPIView,
    SmartHISUpdateAPIView,
    SmartHISDeleteAPIView,
    PointTypeListAPIView,
    HISPointCreateAPIView,
    HISPointUpdateAPIView,
    HISPointDeleteAPIView,
    HISPointListAPIView,
    HISPointDetailsView,
)

__all__ = [
    # Auth
    'UserLoginView',
    'LogoutAPIView',
    'TokenRefreshView',
    # License
    'LicenseManagementView',
    # Users
    'AdminCreateUserAPIView',
    'InvestorCreateUserAPIView',
    'FarmAdminCreateUserAPIView',
    'UserInfoView',
    'UserListAPIView',
    'UserUpdateAPIView',
    'UserDeleteAPIView',
    # Farms
    'FarmCreateAPIView',
    'FarmUpdateAPIView',
    'FarmDeleteAPIView',
    'FarmListAPIView',
    'FarmDetailsView',
    # Turbines
    'TurbineCreateAPIView',
    'TurbineUpdateAPIView',
    'TurbineDeleteAPIView',
    'TurbineListAPIView',
    'TurbineDetailsView',
    # Acquisition - SmartHIS
    'SmartHISCreateAPIView',
    'SmartHISUpdateAPIView',
    'SmartHISDeleteAPIView',
    # Acquisition - PointType (chỉ List để reference)
    'PointTypeListAPIView',
    # Acquisition - HISPoint
    'HISPointCreateAPIView',
    'HISPointUpdateAPIView',
    'HISPointDeleteAPIView',
    'HISPointListAPIView',
    'HISPointDetailsView',
]

