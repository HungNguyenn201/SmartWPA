"""License management views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from permissions.models import Account, License
from permissions.views import IsAdminMain
from facilities.models import Farm
from datetime import datetime, timedelta
from api_gateway.management.common.helpers import create_or_get_investor
from api_gateway.management.users.validators import validate_user_input
from django.db import transaction
import logging

logger = logging.getLogger(__name__)

class LicenseManagementView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, IsAdminMain]

    def post(self, request, investor_id=None):
        """Tạo license cho Investor mới"""
        if investor_id:
            return Response({
                "success": False,
                "error": "POST method not allowed for this endpoint",
                "code": "METHOD_NOT_ALLOWED"
            }, status=status.HTTP_405_METHOD_NOT_ALLOWED)

        email = request.data.get('email')
        username = request.data.get('username')
        password = request.data.get('password')
        is_permanent = request.data.get('is_permanent', True)
        expiry_date = request.data.get('expiry_date', None)

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

        # Khởi tạo expiry
        expiry = None
        if not is_permanent and expiry_date:
            try:
                expiry = datetime.now() + timedelta(days=int(expiry_date))
            except (ValueError, TypeError):
                logger.error(f"Invalid expiry_date format: {expiry_date}")
                return Response({
                    "success": False,
                    "error": "Invalid expiry_date format. Must be a number of days.",
                    "code": "INVALID_EXPIRY_DATE"
                }, status=status.HTTP_400_BAD_REQUEST)

        # Tạo investor_profile trước khi tạo Account
        with transaction.atomic():
            investor_obj = create_or_get_investor(email, username, True)
            
            # Tạo user with investor role và gán investor_profile ngay
            investor = Account.objects.create_user(
                email=email,
                username=username,
                password=password,
                role='investor',
                investor_profile=investor_obj
            )

            # Cập nhật license nếu có thay đổi
            try:
                license_obj = License.objects.get(investor=investor_obj)
                if not is_permanent and expiry:
                    license_obj.expiry_date = expiry
                    license_obj.is_permanent = False
                elif is_permanent:
                    license_obj.is_permanent = True
                    license_obj.expiry_date = None
                license_obj.save()
                license_key = license_obj.key
            except License.DoesNotExist:
                # Nếu chưa có license, tạo mới
                license_obj = investor_obj.generate_license(is_permanent=is_permanent, expiry_date=expiry)
                license_key = license_obj.key

        return Response({
            "success": True,
            "data": {
                "username": investor.username,
                "license_key": license_key,
                "is_permanent": is_permanent,
                "expiry_date": expiry.strftime('%Y-%m-%d %H:%M:%S') if expiry else "Never"
            }
        }, status=status.HTTP_201_CREATED)

    def patch(self, request, investor_id):
        """Gia hạn hoặc vô hiệu hóa license"""
        try:
            # Tìm account trước
            investor_account = Account.objects.get(id=investor_id, role='investor')
            
            # Kiểm tra xem account này có liên kết với Investor không
            if not investor_account.investor_profile:
                return Response({
                    "success": False,
                    "error": "No investor profile associated with this account",
                    "code": "NO_INVESTOR_PROFILE"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Lấy License từ đối tượng Investor
            try:
                license = License.objects.get(investor=investor_account.investor_profile)
            except License.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "License not found for this investor",
                    "code": "LICENSE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
                
        except Account.DoesNotExist:
            return Response({
                "success": False,
                "error": "Investor account not found",
                "code": "INVESTOR_NOT_FOUND"
            }, status=status.HTTP_404_NOT_FOUND)

        action = request.data.get('action', None)
        if not action:
            return Response({
                "success": False,
                "error": "Action is required",
                "code": "MISSING_ACTION"
            }, status=status.HTTP_400_BAD_REQUEST)

        if action == 'extend':
            days = request.data.get('days')
            if days is None:
                return Response({
                    "success": False,
                    "error": "Days parameter is required for extend action",
                    "code": "MISSING_DAYS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                days = int(days)
                if days <= 0:
                    return Response({
                        "success": False,
                        "error": "Days must be greater than 0",
                        "code": "INVALID_DAYS"
                    }, status=status.HTTP_400_BAD_REQUEST)
            except ValueError:
                return Response({
                    "success": False,
                    "error": "Days must be a valid number",
                    "code": "INVALID_DAYS"
                }, status=status.HTTP_400_BAD_REQUEST)

            license.expiry_date = datetime.now() + timedelta(days=days)
            license.is_permanent = False
            
            # Tự động cập nhật token của tất cả user liên quan đến investor này
            try:
                # Lấy danh sách farm liên quan đến investor này
                farms = Farm.objects.filter(investor=investor_account.investor_profile)
                
                # Tìm tất cả farm_admin và staff của các farm này
                farm_users = Account.objects.filter(farm__in=farms)
                
                # Log thông tin
                logger.warning(f"License extended for investor {investor_account.username}. Affected farms: {farms.count()}, Affected users: {farm_users.count()}")
                
            except Exception as e:
                logger.error(f"Error updating related users for investor {investor_account.username}: {str(e)}")
        
        elif action == 'disable':
            license.expiry_date = datetime.now() - timedelta(days=1)
            license.is_permanent = False
        elif action == 'make_permanent':
            license.expiry_date = None
            license.is_permanent = True
        else:
            return Response({
                "success": False,
                "error": "Invalid action. Must be one of: extend, disable, make_permanent",
                "code": "INVALID_ACTION"
            }, status=status.HTTP_400_BAD_REQUEST)

        license.save()
        return Response({
            "success": True,
            "data": {
                "new_expiry": license.expiry_date,
                "is_permanent": license.is_permanent
            }
        }, status=status.HTTP_200_OK)