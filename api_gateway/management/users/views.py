"""User management views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from permissions.models import Account, License
from permissions.views import CanDeleteUser
from facilities.models import Farm, Turbines, Investor
from django.db import IntegrityError
from django.db import transaction
from django.db.models import Q
from api_gateway.management.common.helpers import create_or_get_investor, UserValidationError
from api_gateway.management.users.validators import validate_user_input
import logging

logger = logging.getLogger(__name__)

# ----------------------- CREATE USER ----------------------------
class AdminCreateUserAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            # Chỉ admin main mới được phép tạo user
            if request.user.role != 'admin':
                return Response({
                    "success": False,
                    "error": "Only Main Admin can create users",
                    "code": "PERMISSION_DENIED"
                }, status=status.HTTP_403_FORBIDDEN)

            # Lấy dữ liệu đầu vào
            username = request.data.get('username')
            email = request.data.get('email')
            password = request.data.get('password')
            role = request.data.get('role')
            investor_id = request.data.get('investor_id')
            farm_id = request.data.get('farm_id')

            # Kiểm tra thiếu thông tin
            if not email or not username or not password or not role:
                return Response({
                    "success": False,
                    "error": "Missing required fields",
                    "code": "MISSING_FIELDS"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Validate input
            validation_errors = validate_user_input(username=username, email=email, password=password)
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Validate investor_id nếu có
            if investor_id is not None:
                try:
                    investor_id = int(investor_id)
                except (ValueError, TypeError):
                    return Response({
                        "success": False,
                        "error": "Investor ID must be a valid integer",
                        "code": "INVALID_INVESTOR_ID"
                    }, status=status.HTTP_400_BAD_REQUEST)

            investor = None
            farm = None
                
            # Xử lý tạo Farm Admin hoặc Staff
            if role in ['farm_admin', 'staff']:
                if not farm_id:
                    return Response({
                        "success": False,
                        "error": "Farm ID is required for this role",
                        "code": "MISSING_FARM_ID"
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Lấy farm đúng với investor tương ứng
                try:
                    farm = Farm.objects.get(id=farm_id)
                    if farm.investor:
                        investor = farm.investor
                    else:
                        logger.warning(f"Farm {farm_id} does not have an investor assigned")
                except Farm.DoesNotExist:
                    return Response({
                        "success": False,
                        "error": "Farm does not exist",
                        "code": "FARM_NOT_FOUND"
                    }, status=status.HTTP_404_NOT_FOUND)

            # Tạo user dựa trên role
            user = None
            if role == 'investor':
                # Tạo investor và license trong một transaction
                with transaction.atomic():
                    # Xử lý liên kết với Investor object TRƯỚC khi tạo user
                    investor_obj = None
                    if investor_id:
                        # Ưu tiên: Nếu có investor_id, lấy Investor object theo ID
                        try:
                            investor_obj = Investor.objects.get(id=investor_id)
                            # Đảm bảo có license
                            investor_obj.generate_license()
                        except Investor.DoesNotExist:
                            return Response({
                                "success": False,
                                "error": "Investor does not exist",
                                "code": "INVESTOR_NOT_FOUND"
                            }, status=status.HTTP_404_NOT_FOUND)
                    else:
                        # Fallback: Nếu không có investor_id, tìm hoặc tạo theo email
                        # Tạo user tạm để lấy is_active, sau đó sẽ cập nhật
                        investor_obj = create_or_get_investor(email, username, True)
                    
                    # Tạo user mới với investor_profile đã được gán ngay từ đầu
                    user = Account.objects.create_user(
                        username=username,
                        email=email,
                        password=password,
                        role='investor',
                        manager=request.user,  # Thêm manager_id cho investor user
                        investor_profile=investor_obj  # Gán investor_profile ngay khi tạo
                    )
            else:
                # Tạo user mới với các thông tin đã validate
                user = Account.objects.create_user(
                    username=username,
                    email=email,
                    password=password,
                    role=role,
                    farm=farm,
                    manager=request.user,  # Gán Admin là manager
                    investor_profile=investor  # Thêm investor_profile cho farm_admin hay staff
                )

            return Response({
                "success": True,
                "message": f"User '{username}' with role '{role}' created successfully",
                "data": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "role": user.role,
                    "manager_id": user.manager.id if user.manager else None,
                    "farm": {
                        "id": farm.id,
                        "name": farm.name
                    } if farm else None,
                    "investor_profile_id": user.investor_profile.id if user.investor_profile else None
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return Response({
                "success": False,
                "error": "An error occurred while creating the user",
                "code": "USER_CREATION_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class InvestorCreateUserAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            # Kiểm tra quyền
            if request.user.role != 'investor':
                return Response({
                    "success": False,
                    "error": "Only Investor can create users",
                    "code": "PERMISSION_DENIED"
                }, status=status.HTTP_403_FORBIDDEN)

            # Lấy dữ liệu đầu vào
            username = request.data.get('username')
            email = request.data.get('email')
            password = request.data.get('password')
            role = request.data.get('role')
            farm_id = request.data.get('farm_id')

            # Kiểm tra thiếu thông tin
            if not email or not username or not password or not role or not farm_id:
                return Response({
                    "success": False,
                    "error": "Missing required fields",
                    "code": "MISSING_FIELDS"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Validate input
            validation_errors = validate_user_input(username=username, email=email, password=password)
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Kiểm tra farm_id nếu có
            farm = None
            if farm_id:
                try:
                    # Tìm farm thuộc investor_profile của user hiện tại
                    farm = Farm.objects.get(id=farm_id, investor=request.user.investor_profile)
                except Farm.DoesNotExist:
                    return Response({
                        "success": False,
                        "error": "Farm does not exist or you don't have access to it",
                        "code": "FARM_NOT_FOUND"
                    }, status=status.HTTP_404_NOT_FOUND)

            # Lấy investor profile của user hiện tại
            investor_profile = request.user.investor_profile
            if not investor_profile:
                return Response({
                    "success": False,
                    "error": "Your account is not linked to an investor profile",
                    "code": "NO_INVESTOR_PROFILE"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Tạo user với transaction để đảm bảo tính nhất quán
            with transaction.atomic():
                user = Account.objects.create_user(
                    username=username,
                    email=email,
                    password=password,
                    role=role,
                    farm=farm,
                    manager=request.user,
                    investor_profile=investor_profile  # Thêm investor_profile
                )
            
            return Response({
                "success": True,
                "message": f"User '{username}' with role '{role}' created successfully",
                "data": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "role": user.role,
                    "manager_id": user.manager.id,
                    "investor_profile_id": user.investor_profile.id if user.investor_profile else None,
                    "farm": {
                        "id": farm.id,
                        "name": farm.name
                    }
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error creating user by investor: {str(e)}")
            return Response({
                "success": False,
                "error": "An error occurred while creating the user",
                "code": "USER_CREATION_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class FarmAdminCreateUserAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            # Kiểm tra quyền
            if request.user.role != 'farm_admin':
                return Response({
                    "success": False,
                    "error": "Only Farm Admin can create users",
                    "code": "PERMISSION_DENIED"
                }, status=status.HTTP_403_FORBIDDEN)

            # Lấy dữ liệu đầu vào
            username = request.data.get('username')
            email = request.data.get('email')
            password = request.data.get('password')

            # Kiểm tra thiếu thông tin
            if not email or not username or not password:
                return Response({
                    "success": False,
                    "error": "Missing required fields",
                    "code": "MISSING_FIELDS"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Validate input
            validation_errors = validate_user_input(username=username, email=email, password=password)
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Kiểm tra farm đã được gán cho farm admin hay chưa
            if not request.user.farm:
                return Response({
                    "success": False,
                    "error": "You are not assigned to any farm",
                    "code": "NO_FARM_ASSIGNED"
                }, status=status.HTTP_400_BAD_REQUEST)
                
            # Kiểm tra farm và investor
            farm = request.user.farm
            investor = None
            if farm and farm.investor:
                investor = farm.investor
            else:
                logger.warning(f"Farm {farm.id if farm else 'Unknown'} does not have an investor assigned")
            
            # Chỉ tạo user nếu farm tồn tại
            if not farm:
                return Response({
                    "success": False,
                    "error": "Invalid farm assignment",
                    "code": "INVALID_FARM"
                }, status=status.HTTP_400_BAD_REQUEST)
                
            # Tạo user với transaction để đảm bảo tính nhất quán
            with transaction.atomic():
                user = Account.objects.create_user(
                    username=username,
                    email=email,
                    password=password,
                    role='staff',
                    farm=farm,
                    manager=request.user,  # Gán Farm Admin là manager
                    investor_profile=investor  # Thêm investor_profile
                )

            return Response({
                "success": True,
                "message": f"Staff user '{username}' created successfully",
                "data": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "role": "staff",
                    "manager_id": user.manager.id,
                    "investor_profile_id": user.investor_profile.id if user.investor_profile else None,
                    "farm": {
                        "id": farm.id,
                        "name": farm.name
                    }
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error creating staff by farm admin: {str(e)}")
            return Response({
                "success": False,
                "error": "An error occurred while creating the staff user",
                "code": "USER_CREATION_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ------------------------USER MANAGEMENT ------------------------
class UserInfoView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]
    
    def get(self, request, user_id=None):
        """
        Lấy thông tin người dùng dựa trên role:
        - Admin có thể xem thông tin tất cả người dùng
        - Investor có thể xem thông tin của mình và của farm_admin, staff thuộc các farm của họ
        - Farm Admin có thể xem thông tin của mình và staff thuộc farm của họ
        - Staff chỉ có thể xem thông tin của mình
        """
        current_user = request.user
        
        # Nếu không có user_id, trả về thông tin của người dùng hiện tại
        if not user_id:
            return self._get_user_info(current_user)
        
        try:
            target_user = Account.objects.get(id=user_id)
        except Account.DoesNotExist:
            return Response({
                "success": False,
                "error": "User not found",
                "code": "USER_NOT_FOUND"
            }, status=status.HTTP_404_NOT_FOUND)
            
        # Kiểm tra phân quyền
        has_permission = False
        
        # Admin có thể xem tất cả
        if current_user.role == 'admin':
            has_permission = True
            
        # Investor có thể xem các farm_admin và staff thuộc farm của họ
        elif current_user.role == 'investor' and current_user.investor_profile:
            # Kiểm tra license hợp lệ
            try:
                license_obj = License.objects.get(investor=current_user.investor_profile)
                if not license_obj.is_valid() and target_user.id != current_user.id:
                    return Response({
                        "success": False,
                        "error": "Your license has expired",
                        "code": "LICENSE_EXPIRED"
                    }, status=status.HTTP_403_FORBIDDEN)
            except License.DoesNotExist:
                if target_user.id != current_user.id:
                    return Response({
                        "success": False,
                        "error": "No valid license found",
                        "code": "NO_LICENSE"
                    }, status=status.HTTP_403_FORBIDDEN)
            
            if target_user.role == 'investor' and target_user.id == current_user.id:
                # Investor có thể xem thông tin của chính mình
                has_permission = True
            elif target_user.role in ['farm_admin', 'staff']:
                # Kiểm tra xem farm của user có thuộc investor này không
                investor_farms = Farm.objects.filter(investor=current_user.investor_profile)
                if target_user.farm and target_user.farm in investor_farms:
                    has_permission = True
                    
        # Farm Admin có thể xem các staff thuộc farm của họ
        elif current_user.role == 'farm_admin' and current_user.farm:
            if target_user.id == current_user.id:
                # Farm Admin có thể xem thông tin của chính mình
                has_permission = True
            elif target_user.role == 'staff' and target_user.farm and target_user.farm.id == current_user.farm.id:
                has_permission = True
                
        # Staff chỉ có thể xem thông tin của chính mình
        elif current_user.role == 'staff':
            has_permission = target_user.id == current_user.id
            
        if not has_permission:
            return Response({
                "success": False,
                "error": "You don't have permission to view this user's information",
                "code": "PERMISSION_DENIED"
            }, status=status.HTTP_403_FORBIDDEN)
            
        return self._get_user_info(target_user)
        
    def _get_user_info(self, user):
        """Trả về thông tin chi tiết của user"""
        user_data = {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "is_active": user.is_active,
            "date_created": user.date_created,
            "last_login": user.last_login
        }
        
        # Thêm thông tin manager nếu có
        if user.manager:
            user_data["manager"] = {
                "id": user.manager.id,
                "username": user.manager.username,
                "role": user.manager.role
            }
        
        # Thêm thông tin khác tùy theo role
        if user.role == 'investor' and user.investor_profile:
            license_info = None
            try:
                license_obj = License.objects.get(investor=user.investor_profile)
                license_info = {
                    "key": license_obj.key,
                    "is_permanent": license_obj.is_permanent,
                    "expiry_date": license_obj.expiry_date,
                    "is_valid": license_obj.is_valid()
                }
            except License.DoesNotExist:
                license_info = {
                    "key": None,
                    "is_permanent": False,
                    "expiry_date": None,
                    "is_valid": False,
                    "error": "No license found"
                }
            except Exception as e:
                logger.warning(f"Error getting license info for investor {user.id}: {str(e)}")
                license_info = {
                    "key": None,
                    "is_permanent": False,
                    "expiry_date": None,
                    "is_valid": False,
                    "error": "Error fetching license information"
                }
                
            user_data["investor_info"] = {
                "id": user.investor_profile.id,
                "name": user.investor_profile.name,
                "email": user.investor_profile.email,
                "license_key": user.investor_profile.license_account.key if hasattr(user.investor_profile, 'license_account') else None,
                "is_active": user.investor_profile.is_active,
                "license": license_info
            }
            
            # Thêm danh sách các farm thuộc investor
            try:
                farms = Farm.objects.filter(investor=user.investor_profile)
                user_data["farms"] = [{
                    "id": farm.id,
                    "name": farm.name,
                    "address": farm.address,
                    "capacity": farm.capacity
                } for farm in farms]
            except Exception as e:
                logger.warning(f"Error fetching farms for investor {user.id}: {str(e)}")
                user_data["farms"] = []
                user_data["farms_error"] = "Error fetching farm information"
            
        elif user.role in ['farm_admin', 'staff'] and user.farm:
            user_data["farm"] = {
                "id": user.farm.id,
                "name": user.farm.name,
                "address": user.farm.address,
                "capacity": user.farm.capacity
            }
            
            if user.farm.investor:
                user_data["investor"] = {
                    "id": user.farm.investor.id,
                    "name": user.farm.investor.name,
                    "email": user.farm.investor.email
                }
                
        return Response({
            "success": True,
            "data": user_data
        }, status=status.HTTP_200_OK)

class UserListAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            if request.user.role == "admin":
                # Admin xem tất cả user trừ admin khác
                users = Account.objects.exclude(role="admin").select_related('farm', 'farm__investor', 'investor_profile')
                
            elif request.user.role == "investor":
                # Kiểm tra license hợp lệ cho investor
                if not request.user.investor_profile:
                    return Response({
                        "success": False,
                        "error": "No investor profile found for your account",
                        "code": "NO_INVESTOR_PROFILE"
                    }, status=status.HTTP_403_FORBIDDEN)
                
                try:
                    license_obj = License.objects.get(investor=request.user.investor_profile)
                    if not license_obj.is_valid():
                        return Response({
                            "success": False,
                            "error": "Your license has expired",
                            "code": "LICENSE_EXPIRED"
                        }, status=status.HTTP_403_FORBIDDEN)
                except License.DoesNotExist:
                    return Response({
                        "success": False,
                        "error": "No valid license found",
                        "code": "NO_LICENSE"
                    }, status=status.HTTP_403_FORBIDDEN)
                
                # Lấy danh sách farm thuộc investor
                farms = Farm.objects.filter(investor=request.user.investor_profile)
                farm_ids = [farm.id for farm in farms]
                
                # Lấy các user là farm_admin hoặc staff thuộc các farm đó
                # hoặc được quản lý bởi investor này
                users = Account.objects.filter(
                    Q(farm_id__in=farm_ids) |  
                    Q(manager=request.user)
                ).select_related('farm', 'farm__investor')
                
            elif request.user.role == "farm_admin":
                # Farm admin chỉ xem staff của farm mình
                if not request.user.farm:
                    return Response({
                        "success": False,
                        "error": "You are not assigned to any farm",
                        "code": "NO_FARM_ASSIGNED"
                    }, status=status.HTTP_403_FORBIDDEN)
                users = Account.objects.filter(farm=request.user.farm, role='staff').select_related('farm', 'farm__investor')
            else:
                return Response({
                    "success": False,
                    "error": "You do not have permission to view users",
                    "code": "INVALID_ROLE"
                }, status=status.HTTP_403_FORBIDDEN)
            
            user_list = []
            for user in users:
                user_data = {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "role": user.role,
                    "is_active": user.is_active,
                    "farm": {
                        "id": user.farm.id,
                        "name": user.farm.name
                    } if user.farm else None,
                    "investor": {
                        "id": user.farm.investor.id,
                        "name": user.farm.investor.name
                    } if user.farm and user.farm.investor else None,
                    "created_at": user.date_created.isoformat() if hasattr(user, 'date_created') and user.date_created else None
                }
                
                # Thêm thông tin license nếu user là investor
                if user.role == 'investor' and hasattr(user, 'investor_profile') and user.investor_profile:
                    try:
                        license_obj = License.objects.get(investor=user.investor_profile)
                        user_data["license_info"] = {
                            "is_valid": license_obj.is_valid(),
                            "expiry_date": license_obj.expiry_date.isoformat() if license_obj.expiry_date else None,
                            "is_permanent": license_obj.is_permanent
                        }
                    except License.DoesNotExist:
                        user_data["license_info"] = {
                            "is_valid": False,
                            "expiry_date": None,
                            "is_permanent": False
                        }
                    except Exception as license_error:
                        logger.warning(f"Error getting license info for user {user.id}: {str(license_error)}")
                        user_data["license_info"] = {
                            "is_valid": False,
                            "expiry_date": None,
                            "is_permanent": False,
                            "error": "Error fetching license information"
                        }
                
                user_list.append(user_data)
            
            return Response({
                "success": True,
                "data": user_list
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error in UserListAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserUpdateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def validate_user_data(self, request, user):
        """Validate user update data"""
        username = request.data.get('username') if 'username' in request.data else None
        email = request.data.get('email') if 'email' in request.data else None
        
        validation_errors = validate_user_input(
            username=username,
            email=email,
            password=None,
            exclude_user_id=user.id
        )
        
        if validation_errors:
            raise UserValidationError(validation_errors[0]["error"])

    def put(self, request, user_id=None):
        try:
            if not user_id:
                return Response({
                    "success": False,
                    "error": "User ID is required",
                    "code": "MISSING_USER_ID"
                }, status=status.HTTP_400_BAD_REQUEST)
                
            try:
                user = Account.objects.get(id=user_id)
            except Account.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "User does not exist",
                    "code": "USER_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Kiểm tra quyền
            if request.user.role == "admin":
                # Admin có thể chỉnh sửa bất kỳ user trừ admin khác
                if user.role == "admin" and request.user.id != user.id:
                    return Response({
                        "success": False,
                        "error": "You cannot edit other admin accounts",
                        "code": "ADMIN_EDIT_DENIED"
                    }, status=status.HTTP_403_FORBIDDEN)
                    
            elif request.user.role == "investor":
                # Investor có thể chỉnh sửa farm admin và staff trong farm của mình
                if not request.user.investor_profile:
                    return Response({
                        "success": False,
                        "error": "No investor profile found for your account",
                        "code": "NO_INVESTOR_PROFILE"
                    }, status=status.HTTP_403_FORBIDDEN)
                
                # Investor chỉ có thể chỉnh sửa user thuộc farm của mình hoặc chính mình
                if user.id == request.user.id:
                    pass  # Cho phép chỉnh sửa chính mình
                elif user.role in ['farm_admin', 'staff']:
                    if not user.farm or user.farm.investor != request.user.investor_profile:
                        return Response({
                            "success": False,
                            "error": "You can only edit users of your own farms",
                            "code": "USER_OWNERSHIP_ERROR"
                        }, status=status.HTTP_403_FORBIDDEN)
                else:
                    return Response({
                        "success": False,
                        "error": "You can only edit farm_admin and staff users",
                        "code": "PERMISSION_DENIED"
                    }, status=status.HTTP_403_FORBIDDEN)
                    
            elif request.user.role == "farm_admin":
                # Farm admin chỉ có thể chỉnh sửa staff trong farm của mình hoặc chính mình
                if user.id == request.user.id:
                    pass  # Cho phép chỉnh sửa chính mình
                elif user.role != "staff" or not user.farm or user.farm != request.user.farm:
                    return Response({
                        "success": False,
                        "error": "You can only edit staff of your farm",
                        "code": "STAFF_EDIT_DENIED"
                    }, status=status.HTTP_403_FORBIDDEN)
            else:
                # Staff chỉ có thể chỉnh sửa chính mình
                if user.id != request.user.id:
                    return Response({
                        "success": False,
                        "error": "You can only edit your own account",
                        "code": "PERMISSION_DENIED"
                    }, status=status.HTTP_403_FORBIDDEN)
            
            # Validate dữ liệu
            self.validate_user_data(request, user)
            
            # Cập nhật user
            if 'username' in request.data:
                user.username = request.data.get('username')
                
            if 'email' in request.data:
                user.email = request.data.get('email')
                
            if 'password' in request.data and request.data.get('password'):
                user.set_password(request.data.get('password'))
                
            if 'is_active' in request.data and request.user.role == "admin":
                user.is_active = request.data.get('is_active')
                
            user.save()
            
            return Response({
                "success": True,
                "message": "User updated successfully",
                "data": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "role": user.role,
                    "is_active": user.is_active,
                    "farm": {
                        "id": user.farm.id,
                        "name": user.farm.name
                    } if user.farm else None
                }
            }, status=status.HTTP_200_OK)

        except UserValidationError as e:
            return Response({
                "success": False,
                "error": str(e),
                "code": "VALIDATION_ERROR"
            }, status=status.HTTP_400_BAD_REQUEST)
            
        except IntegrityError as e:
            logger.error(f"Database integrity error in UserUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in UserUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserDeleteAPIView(APIView):
    """API để xóa user"""
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanDeleteUser]
    
    def delete(self, request, user_id):
        try:
            user = Account.objects.get(id=user_id)
            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, user):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to delete this user",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)
            
            # Xử lý xóa user theo role
            if user.role == "investor":
                # Xóa investor profile và tất cả farm liên quan
                if hasattr(user, 'investor_profile'):
                    investor = user.investor_profile
                    # Xóa tất cả farm thuộc investor
                    farms = Farm.objects.filter(investor=investor)
                    for farm in farms:
                        # Xóa tất cả turbine trong farm
                        turbines = Turbines.objects.filter(farm=farm)
                        turbines.delete()
                    farms.delete()
                    # Xóa investor
                    investor.delete()
            
            elif user.role == "farm_admin":
                # Xóa farm liên quan
                if hasattr(user, 'farm'):
                    farm = user.farm
                    # Xóa tất cả turbine trong farm
                    turbines = Turbines.objects.filter(farm=farm)
                    turbines.delete()
                    # Xóa farm
                    farm.delete()
            
            elif user.role == "staff":
                # Chỉ xóa user staff
                pass
            
            # Xóa user
            user.delete()
            
            return Response({
                "success": True,
                "message": "User deleted successfully"
            })
            
        except Account.DoesNotExist:
            return Response({
                "success": False,
                "error": "User not found",
                "code": "USER_NOT_FOUND"
            }, status=status.HTTP_404_NOT_FOUND)
            
        except IntegrityError as e:
            logger.error(f"Database integrity error in UserDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Error deleting user: {str(e)}")
            return Response({
                "success": False,
                "error": f"An unexpected error occurred: {str(e)}",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

