"""API Gateway URL Configuration"""
from django.urls import path, include
from api_gateway.management import (
    # Auth
    UserLoginView,
    LogoutAPIView,
    TokenRefreshView,
    # License
    LicenseManagementView,
    # Users
    AdminCreateUserAPIView,
    InvestorCreateUserAPIView,
    FarmAdminCreateUserAPIView,
    UserInfoView,
    UserListAPIView,
    UserUpdateAPIView,
    UserDeleteAPIView,
    # Farms
    FarmCreateAPIView,
    FarmUpdateAPIView,
    FarmDeleteAPIView,
    FarmListAPIView,
    FarmDetailsView,
    # Turbines
    TurbineCreateAPIView,
    TurbineUpdateAPIView,
    TurbineDeleteAPIView,
    TurbineListAPIView,
    TurbineDetailsView,
    # Acquisition - SmartHIS
    SmartHISCreateAPIView,
    SmartHISUpdateAPIView,
    SmartHISDeleteAPIView,
    # Acquisition - PointType (chỉ List để reference)
    PointTypeListAPIView,
    # Acquisition - HISPoint
    HISPointCreateAPIView,
    HISPointUpdateAPIView,
    HISPointDeleteAPIView,
    HISPointListAPIView,
    HISPointDetailsView,
)
from api_gateway.turbines_analysis.classification_rate import ClassificationRateAPIView
from api_gateway.turbines_analysis.distribution import DistributionAPIView
from api_gateway.turbines_analysis.indicators import TurbineIndicatorAPIView, FarmIndicatorAPIView

urlpatterns = [
    # Authentication endpoints
    path('api/auth/login/', UserLoginView.as_view(), name='user-login'),
    path('api/auth/logout/', LogoutAPIView.as_view(), name='user-logout'),
    path('api/auth/refresh/', TokenRefreshView.as_view(), name='token-refresh'),
    
    # License management endpoints
    path('api/license/', LicenseManagementView.as_view(), name='license-create'),
    path('api/license/<int:investor_id>/', LicenseManagementView.as_view(), name='license-update'),
    
    # User management endpoints
    path('api/users/', UserListAPIView.as_view(), name='user-list'),
    path('api/users/create/', AdminCreateUserAPIView.as_view(), name='admin-create-user'),
    path('api/users/investor/create/', InvestorCreateUserAPIView.as_view(), name='investor-create-user'),
    path('api/users/farm-admin/create/', FarmAdminCreateUserAPIView.as_view(), name='farm-admin-create-user'),
    path('api/users/<int:user_id>/', UserInfoView.as_view(), name='user-info'),
    path('api/users/<int:user_id>/update/', UserUpdateAPIView.as_view(), name='user-update'),
    path('api/users/<int:user_id>/delete/', UserDeleteAPIView.as_view(), name='user-delete'),
    
    # Farm management endpoints
    path('api/farms/', FarmListAPIView.as_view(), name='farm-list'),
    path('api/farms/create/', FarmCreateAPIView.as_view(), name='farm-create'),
    path('api/farms/<int:farm_id>/', FarmDetailsView.as_view(), name='farm-details'),
    path('api/farms/<int:farm_id>/update/', FarmUpdateAPIView.as_view(), name='farm-update'),
    path('api/farms/<int:farm_id>/delete/', FarmDeleteAPIView.as_view(), name='farm-delete'),
    
    # Turbine management endpoints
    path('api/turbines/', TurbineListAPIView.as_view(), name='turbine-list'),
    path('api/farms/<int:farm_id>/turbines/create/', TurbineCreateAPIView.as_view(), name='turbine-create'),
    path('api/turbines/<int:turbine_id>/', TurbineDetailsView.as_view(), name='turbine-details'),
    path('api/turbines/<int:turbine_id>/update/', TurbineUpdateAPIView.as_view(), name='turbine-update'),
    path('api/turbines/<int:turbine_id>/delete/', TurbineDeleteAPIView.as_view(), name='turbine-delete'),
    
    # SmartHIS management endpoints
    path('api/smart-his/create/', SmartHISCreateAPIView.as_view(), name='smart-his-create'),
    path('api/smart-his/<int:smart_his_id>/update/', SmartHISUpdateAPIView.as_view(), name='smart-his-update'),
    path('api/smart-his/<int:smart_his_id>/delete/', SmartHISDeleteAPIView.as_view(), name='smart-his-delete'),
    
    # PointType endpoints (chỉ List để reference khi tạo HISPoint)
    path('api/point-types/', PointTypeListAPIView.as_view(), name='point-type-list'),
    
    # HISPoint management endpoints
    path('api/his-points/', HISPointListAPIView.as_view(), name='his-point-list'),
    path('api/his-points/create/', HISPointCreateAPIView.as_view(), name='his-point-create'),
    path('api/his-points/<int:his_point_id>/', HISPointDetailsView.as_view(), name='his-point-details'),
    path('api/his-points/<int:his_point_id>/update/', HISPointUpdateAPIView.as_view(), name='his-point-update'),
    path('api/his-points/<int:his_point_id>/delete/', HISPointDeleteAPIView.as_view(), name='his-point-delete'),
    
    # Turbine analysis endpoints
    path('api/turbines/<int:turbine_id>/classification-rate/', ClassificationRateAPIView.as_view(), name='classification-rate'),
    path('api/turbines/<int:turbine_id>/distribution/', DistributionAPIView.as_view(), name='distribution'),
    path('api/turbines/<int:turbine_id>/indicators/', TurbineIndicatorAPIView.as_view(), name='turbine-indicators'),
    
    # Farm analysis endpoints
    path('api/farms/<int:farm_id>/indicators/', FarmIndicatorAPIView.as_view(), name='farm-indicators'),
]

