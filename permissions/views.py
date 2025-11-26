from rest_framework.permissions import BasePermission

class IsAdminMain(BasePermission):
    """Chỉ Main Admin"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role == 'admin'
    
class IsInvestor(BasePermission):
    """Chỉ Investor"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role == 'investor'
    
class IsFarmAdmin(BasePermission):
    """Chỉ Farm Admin"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role == 'farm_admin'

class IsStaff(BasePermission):
    """Chỉ Staff/Operator"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role == 'staff'

class IsAdminOrInvestor(BasePermission):
    """Admin hoặc Investor - Admin quản lý Investor"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role in ['admin', 'investor']
    
class IsInvestorOrFarmAdmin(BasePermission):
    """Investor hoặc Farm Admin - Investor quản lý Farm Admin"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role in ['investor', 'farm_admin']

class IsFarmAdminOrStaff(BasePermission):
    """Farm Admin hoặc Staff - Farm Admin quản lý Staff"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role in ['farm_admin', 'staff']

class IsAdminOrFarmAdmin(BasePermission):
    """Admin hoặc Farm Admin - Admin có thể quản lý Farm"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role in ['admin', 'farm_admin']

class IsAdminOrInvestorOrFarmAdmin(BasePermission):
    """Admin, Investor hoặc Farm Admin - Admin có quyền cao nhất"""
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.role in ['admin', 'investor', 'farm_admin']

# Create your views here.
