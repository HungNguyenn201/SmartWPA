from rest_framework.permissions import BasePermission


# Helper functions để tránh lặp code
def get_farm_from_object(obj):
    """Lấy farm từ object (có thể là Farm, Turbine, SmartHIS, HISPoint)"""
    if hasattr(obj, 'farm'):
        return obj.farm
    # Nếu chính nó là Farm
    if obj.__class__.__name__ == 'Farm':
        return obj
    return None


def check_farm_access(current_user, farm, allow_staff=False):
    """Kiểm tra quyền truy cập farm - dùng chung cho tất cả permissions"""
    if not farm:
        return False
    
    # Admin có quyền truy cập tất cả
    if current_user.role == "admin":
        return True
    
    # Investor chỉ có thể truy cập farm của mình
    if current_user.role == "investor":
        if not hasattr(current_user, 'investor_profile') or not current_user.investor_profile:
            return False
        return farm.investor == current_user.investor_profile
    
    # Farm admin và staff chỉ có thể truy cập farm của mình
    if current_user.role in ["farm_admin", "staff"]:
        if not hasattr(current_user, 'farm') or not current_user.farm:
            return False
        return current_user.farm.id == farm.id
    
    return False

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

class CanDeleteUser(BasePermission):
    """Kiểm tra quyền xóa user dựa trên role và mối quan hệ"""
    def has_permission(self, request, view):
        """Kiểm tra quyền cơ bản - chỉ admin, investor, farm_admin mới có thể xóa user"""
        return request.user.is_authenticated and request.user.role in ['admin', 'investor', 'farm_admin']
    
    def has_object_permission(self, request, view, obj):
        """Kiểm tra quyền xóa user cụ thể"""
        current_user = request.user
        target_user = obj       
        # Admin có thể xóa bất kỳ user nào (trừ chính mình)
        if current_user.role == "admin":
            return current_user.id != target_user.id     
        # User không thể xóa chính mình
        if current_user.id == target_user.id:
            return False       
        # Investor chỉ có thể xóa farm_admin và staff thuộc farm của họ
        if current_user.role == "investor":
            if not hasattr(current_user, 'investor_profile') or not current_user.investor_profile:
                return False
            if target_user.role not in ["staff", "farm_admin"]:
                return False
            if not hasattr(target_user, 'farm') or not target_user.farm:
                return False
            return target_user.farm.investor == current_user.investor_profile
        
        # Farm admin chỉ có thể xóa staff trong farm của mình
        if current_user.role == "farm_admin":
            if not hasattr(current_user, 'farm') or not current_user.farm:
                return False
            if target_user.role != "staff":
                return False
            if not hasattr(target_user, 'farm') or not target_user.farm:
                return False
            return target_user.farm == current_user.farm
        
        return False

# Base permission classes để tránh lặp code
class BaseManageFarmRelatedPermission(BasePermission):
    """Base class cho các permission quản lý objects liên quan đến farm"""
    allowed_roles = ['admin', 'investor', 'farm_admin']  # Override trong subclass nếu cần
    
    def has_permission(self, request, view):
        """Kiểm tra quyền cơ bản"""
        return request.user.is_authenticated and request.user.role in self.allowed_roles
    
    def has_object_permission(self, request, view, obj):
        """Kiểm tra quyền quản lý object cụ thể"""
        current_user = request.user
        farm = get_farm_from_object(obj)
        return check_farm_access(current_user, farm, allow_staff=False)


class BaseViewFarmRelatedPermission(BasePermission):
    """Base class cho các permission xem objects liên quan đến farm"""
    
    def has_permission(self, request, view):
        """Tất cả user đã authenticated đều có thể xem"""
        return request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        """Kiểm tra quyền xem object cụ thể"""
        current_user = request.user
        farm = get_farm_from_object(obj)
        return check_farm_access(current_user, farm, allow_staff=True)


# Specific permission classes sử dụng base classes
class CanManageFarm(BaseManageFarmRelatedPermission):
    """Kiểm tra quyền quản lý farm (create, update, delete)"""
    allowed_roles = ['admin', 'investor']  # Farm không cho farm_admin quản lý


class CanViewFarm(BaseViewFarmRelatedPermission):
    """Kiểm tra quyền xem farm"""
    pass


class CanManageTurbine(BaseManageFarmRelatedPermission):
    """Kiểm tra quyền quản lý turbine (create, update, delete)"""
    pass  # Sử dụng allowed_roles mặc định ['admin', 'investor', 'farm_admin']


class CanViewTurbine(BaseViewFarmRelatedPermission):
    """Kiểm tra quyền xem turbine"""
    pass


class CanManageSmartHIS(BaseManageFarmRelatedPermission):
    """Kiểm tra quyền quản lý SmartHIS (create, update, delete)"""
    pass  # Sử dụng allowed_roles mặc định ['admin', 'investor', 'farm_admin']


class CanViewSmartHIS(BaseViewFarmRelatedPermission):
    """Kiểm tra quyền xem SmartHIS"""
    pass


class CanManageHISPoint(BaseManageFarmRelatedPermission):
    """Kiểm tra quyền quản lý HISPoint (create, update, delete)"""
    pass  # Sử dụng allowed_roles mặc định ['admin', 'investor', 'farm_admin']


class CanViewHISPoint(BaseViewFarmRelatedPermission):
    """Kiểm tra quyền xem HISPoint"""
    pass

# Create your views here.
