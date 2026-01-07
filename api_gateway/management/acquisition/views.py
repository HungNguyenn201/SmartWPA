"""Acquisition management views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from acquisition.models import SmartHIS, PointType, HISPoint
from facilities.models import Farm, Turbines
from permissions.views import (
    CanManageSmartHIS,
    CanManageHISPoint, CanViewHISPoint
)
from django.db import IntegrityError
from django.db import transaction
from api_gateway.management.acquisition.validators import (
    validate_smart_his_data,
    validate_his_point_data
)
from api_gateway.management.acquisition.helpers import (
    check_object_permission,
    get_smart_his_serialized_data,
    get_point_type_serialized_data,
    get_his_point_serialized_data
)
import logging

logger = logging.getLogger(__name__)

# -------------------------- SMART HIS MANAGEMENT ------------------------
class SmartHISCreateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageSmartHIS]

    def post(self, request):
        try:
            farm_id = request.data.get('farm_id')
            address = request.data.get('address', '').strip()
            username = request.data.get('username', '').strip()
            password = request.data.get('password', '').strip()
            point_check_expired = request.data.get('point_check_expired', '').strip()

            if not farm_id:
                return Response({
                    "success": False,
                    "error": "Farm ID is required",
                    "code": "MISSING_FARM_ID"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            if not point_check_expired:
                return Response({
                    "success": False,
                    "error": "point_check_expired is required",
                    "code": "MISSING_POINT_CHECK_EXPIRED"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                farm = Farm.objects.get(id=farm_id)
            except Farm.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Farm does not exist",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền - tạo temp SmartHIS để kiểm tra
            temp_smart_his = SmartHIS(farm=farm)
            permission_response = check_object_permission(
                request, self, temp_smart_his,
                "You don't have permission to create SmartHIS for this farm"
            )
            if permission_response:
                return permission_response

            # Validate dữ liệu
            validation_errors = validate_smart_his_data(
                address=address,
                username=username,
                password=password,
                farm=farm
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Test connection nếu được yêu cầu
            test_connection = request.data.get('test_connection', False)
            if test_connection:
                try:
                    from acquisition.smarthis.restful_client import login_and_get_token
                    token = login_and_get_token(address, username, password)
                    
                    if not token:
                        return Response({
                            "success": False,
                            "error": "Connection test failed: Unable to login with provided credentials. Please check address, username, and password.",
                            "code": "CONNECTION_TEST_FAILED"
                        }, status=status.HTTP_400_BAD_REQUEST)
                except Exception as e:
                    logger.error(f"Connection test error in SmartHISCreateAPIView: {str(e)}", exc_info=True)
                    return Response({
                        "success": False,
                        "error": f"Connection test failed: {str(e)}",
                        "code": "CONNECTION_TEST_ERROR"
                    }, status=status.HTTP_400_BAD_REQUEST)

            # Kiểm tra xem farm đã có SmartHIS chưa
            if SmartHIS.objects.filter(farm=farm).exists():
                return Response({
                    "success": False,
                    "error": "This farm already has a SmartHIS configuration",
                    "code": "SMARTHIS_EXISTS"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Tạo SmartHIS mới
            with transaction.atomic():
                smart_his = SmartHIS.objects.create(
                    farm=farm,
                    address=address,
                    username=username,
                    password=password,
                    point_check_expired=point_check_expired
                )
            
            return Response({
                "success": True,
                "message": "SmartHIS created successfully",
                "data": get_smart_his_serialized_data(smart_his)
            }, status=status.HTTP_201_CREATED)

        except IntegrityError as e:
            logger.error(f"Database integrity error in SmartHISCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in SmartHISCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SmartHISUpdateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageSmartHIS]

    def put(self, request, smart_his_id=None):
        try:
            if not smart_his_id:
                return Response({
                    "success": False,
                    "error": "SmartHIS ID is required",
                    "code": "MISSING_SMARTHIS_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                smart_his = SmartHIS.objects.get(id=smart_his_id)
            except SmartHIS.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "SmartHIS does not exist",
                    "code": "SMARTHIS_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền
            permission_response = check_object_permission(
                request, self, smart_his,
                "You don't have permission to update this SmartHIS"
            )
            if permission_response:
                return permission_response

            # Lấy và validate dữ liệu từ request
            update_fields = {}
            if 'address' in request.data:
                update_fields['address'] = request.data.get('address', '').strip()
            if 'username' in request.data:
                update_fields['username'] = request.data.get('username', '').strip()
            if 'password' in request.data:
                update_fields['password'] = request.data.get('password', '').strip()
            if 'point_check_expired' in request.data:
                point_check_expired = request.data.get('point_check_expired', '').strip()
                if not point_check_expired:
                    return Response({
                        "success": False,
                        "error": "point_check_expired cannot be empty",
                        "code": "INVALID_POINT_CHECK_EXPIRED"
                    }, status=status.HTTP_400_BAD_REQUEST)
                update_fields['point_check_expired'] = point_check_expired
            if 'token' in request.data:
                update_fields['token'] = request.data.get('token', '')

            # Validate dữ liệu
            validation_errors = validate_smart_his_data(
                address=update_fields.get('address'),
                username=update_fields.get('username'),
                password=update_fields.get('password'),
                farm=smart_his.farm,
                exclude_smart_his_id=smart_his.id
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Test connection nếu được yêu cầu và có thay đổi connection info
            test_connection = request.data.get('test_connection', False)
            if test_connection:
                # Sử dụng giá trị mới nếu có, nếu không dùng giá trị hiện tại
                test_address = update_fields.get('address', smart_his.address)
                test_username = update_fields.get('username', smart_his.username)
                test_password = update_fields.get('password', smart_his.password)
                
                try:
                    from acquisition.smarthis.restful_client import login_and_get_token
                    token = login_and_get_token(test_address, test_username, test_password)
                    
                    if not token:
                        return Response({
                            "success": False,
                            "error": "Connection test failed: Unable to login with provided credentials. Please check address, username, and password.",
                            "code": "CONNECTION_TEST_FAILED"
                        }, status=status.HTTP_400_BAD_REQUEST)
                except Exception as e:
                    logger.error(f"Connection test error in SmartHISUpdateAPIView: {str(e)}", exc_info=True)
                    return Response({
                        "success": False,
                        "error": f"Connection test failed: {str(e)}",
                        "code": "CONNECTION_TEST_ERROR"
                    }, status=status.HTTP_400_BAD_REQUEST)

            # Update SmartHIS
            for field, value in update_fields.items():
                setattr(smart_his, field, value)
            smart_his.save()

            return Response({
                "success": True,
                "message": "SmartHIS updated successfully",
                "data": get_smart_his_serialized_data(smart_his)
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in SmartHISUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in SmartHISUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SmartHISDeleteAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageSmartHIS]

    def delete(self, request, smart_his_id=None):
        try:
            if not smart_his_id:
                return Response({
                    "success": False,
                    "error": "SmartHIS ID is required",
                    "code": "MISSING_SMARTHIS_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                smart_his = SmartHIS.objects.get(id=smart_his_id)
            except SmartHIS.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "SmartHIS does not exist",
                    "code": "SMARTHIS_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền
            permission_response = check_object_permission(
                request, self, smart_his,
                "You don't have permission to delete this SmartHIS"
            )
            if permission_response:
                return permission_response

            farm_name = smart_his.farm.name if smart_his.farm else "Unknown"
            smart_his.delete()

            return Response({
                "success": True,
                "message": f"SmartHIS for farm '{farm_name}' has been deleted successfully",
                "data": {
                    "id": smart_his_id,
                    "farm_name": farm_name
                }
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in SmartHISDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in SmartHISDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# -------------------------- POINT TYPE MANAGEMENT ------------------------
# PointType chỉ dùng để reference, không cần CRUD - chỉ cần List để chọn khi tạo HISPoint
class PointTypeListAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            level = request.query_params.get('level')  # Optional filter by level
            
            point_types = PointType.objects.all()
            if level:
                point_types = point_types.filter(level=level)
            
            result_list = [get_point_type_serialized_data(pt) for pt in point_types]
            
            return Response({
                "success": True,
                "data": result_list
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error in PointTypeListAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# -------------------------- HIS POINT MANAGEMENT ------------------------
class HISPointCreateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageHISPoint]

    def post(self, request):
        try:
            farm_id = request.data.get('farm_id')
            point_type_id = request.data.get('point_type_id')
            turbine_id = request.data.get('turbine_id', None)
            point_name = request.data.get('point_name', '').strip()
            is_active = request.data.get('is_active', True)

            if not farm_id or not point_type_id or not point_name:
                return Response({
                    "success": False,
                    "error": "Farm ID, PointType ID, and Point Name are required",
                    "code": "MISSING_FIELDS"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                farm = Farm.objects.get(id=farm_id)
            except Farm.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Farm does not exist",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            try:
                point_type = PointType.objects.get(id=point_type_id)
            except PointType.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "PointType does not exist",
                    "code": "POINT_TYPE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            turbine = None
            if turbine_id:
                try:
                    turbine = Turbines.objects.get(id=turbine_id, farm=farm)
                except Turbines.DoesNotExist:
                    return Response({
                        "success": False,
                        "error": "Turbine does not exist or does not belong to this farm",
                        "code": "TURBINE_NOT_FOUND"
                    }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền - tạo temp HISPoint để kiểm tra
            temp_his_point = HISPoint(farm=farm, point_type=point_type, turbine=turbine)
            permission_response = check_object_permission(
                request, self, temp_his_point,
                "You don't have permission to create HISPoint for this farm"
            )
            if permission_response:
                return permission_response

            # Validate dữ liệu
            validation_errors = validate_his_point_data(
                point_name=point_name,
                farm=farm,
                point_type=point_type,
                turbine=turbine
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Tạo HISPoint mới
            with transaction.atomic():
                his_point = HISPoint.objects.create(
                    farm=farm,
                    point_type=point_type,
                    turbine=turbine,
                    point_name=point_name,
                    is_active=is_active
                )
            
            return Response({
                "success": True,
                "message": "HISPoint created successfully",
                "data": get_his_point_serialized_data(his_point)
            }, status=status.HTTP_201_CREATED)

        except IntegrityError as e:
            logger.error(f"Database integrity error in HISPointCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in HISPointCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class HISPointUpdateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageHISPoint]

    def put(self, request, his_point_id=None):
        try:
            if not his_point_id:
                return Response({
                    "success": False,
                    "error": "HISPoint ID is required",
                    "code": "MISSING_HISPOINT_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                his_point = HISPoint.objects.get(id=his_point_id)
            except HISPoint.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "HISPoint does not exist",
                    "code": "HISPOINT_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền
            permission_response = check_object_permission(
                request, self, his_point,
                "You don't have permission to update this HISPoint"
            )
            if permission_response:
                return permission_response

            # Lấy dữ liệu từ request
            point_type = his_point.point_type
            turbine = his_point.turbine
            
            # Xử lý point_type_id nếu có
            if 'point_type_id' in request.data:
                try:
                    point_type = PointType.objects.get(id=request.data.get('point_type_id'))
                except PointType.DoesNotExist:
                    return Response({
                        "success": False,
                        "error": "PointType does not exist",
                        "code": "POINT_TYPE_NOT_FOUND"
                    }, status=status.HTTP_404_NOT_FOUND)
            
            # Xử lý turbine_id nếu có
            if 'turbine_id' in request.data:
                turbine_id = request.data.get('turbine_id')
                if turbine_id:
                    try:
                        turbine = Turbines.objects.get(id=turbine_id, farm=his_point.farm)
                    except Turbines.DoesNotExist:
                        return Response({
                            "success": False,
                            "error": "Turbine does not exist or does not belong to this farm",
                            "code": "TURBINE_NOT_FOUND"
                        }, status=status.HTTP_404_NOT_FOUND)
                else:
                    turbine = None

            # Validate dữ liệu
            validation_errors = validate_his_point_data(
                point_name=request.data.get('point_name', '').strip() if 'point_name' in request.data else None,
                farm=his_point.farm,
                point_type=point_type if 'point_type_id' in request.data else None,
                turbine=turbine if 'turbine_id' in request.data else None,
                exclude_his_point_id=his_point.id
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Update HISPoint
            if 'point_name' in request.data:
                his_point.point_name = request.data.get('point_name', '').strip()
            if 'point_type_id' in request.data:
                his_point.point_type = point_type
            if 'turbine_id' in request.data:
                his_point.turbine = turbine
            if 'is_active' in request.data:
                his_point.is_active = request.data.get('is_active', True)
            his_point.save()

            return Response({
                "success": True,
                "message": "HISPoint updated successfully",
                "data": get_his_point_serialized_data(his_point)
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in HISPointUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in HISPointUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class HISPointDeleteAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageHISPoint]

    def delete(self, request, his_point_id=None):
        try:
            if not his_point_id:
                return Response({
                    "success": False,
                    "error": "HISPoint ID is required",
                    "code": "MISSING_HISPOINT_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                his_point = HISPoint.objects.get(id=his_point_id)
            except HISPoint.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "HISPoint does not exist",
                    "code": "HISPOINT_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền
            permission_response = check_object_permission(
                request, self, his_point,
                "You don't have permission to delete this HISPoint"
            )
            if permission_response:
                return permission_response

            point_name = his_point.point_name
            his_point.delete()

            return Response({
                "success": True,
                "message": f"HISPoint '{point_name}' has been deleted successfully",
                "data": {
                    "id": his_point_id,
                    "point_name": point_name
                }
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in HISPointDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in HISPointDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class HISPointListAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            farm_id = request.query_params.get('farm_id')
            point_type_id = request.query_params.get('point_type_id')
            turbine_id = request.query_params.get('turbine_id')
            level = request.query_params.get('level')  # Filter by point_type level
            
            if request.user.role == "admin":
                his_points = HISPoint.objects.select_related("farm", "farm__investor", "point_type", "turbine").all()

            elif request.user.role == "investor":
                if not request.user.investor_profile:
                    return Response({
                        "success": False,
                        "error": "No investor profile found for your account",
                        "code": "NO_INVESTOR_PROFILE"
                    }, status=status.HTTP_403_FORBIDDEN)
                    
                farms = Farm.objects.filter(investor=request.user.investor_profile)
                his_points = HISPoint.objects.filter(farm__in=farms).select_related('farm', 'farm__investor', 'point_type', 'turbine')

            elif request.user.role in ["farm_admin", "staff"]:
                if not request.user.farm:
                    return Response({
                        "success": False,
                        "error": "You are not assigned to any farm",
                        "code": "NO_FARM_ASSIGNED"
                    }, status=status.HTTP_403_FORBIDDEN)
                his_points = HISPoint.objects.filter(farm=request.user.farm).select_related("farm", "farm__investor", "point_type", "turbine")

            else:
                return Response({
                    "success": False,
                    "error": "You do not have permission",
                    "code": "INVALID_ROLE"
                }, status=status.HTTP_403_FORBIDDEN)
            
            # Apply filters
            if farm_id:
                his_points = his_points.filter(farm_id=farm_id)
            if point_type_id:
                his_points = his_points.filter(point_type_id=point_type_id)
            if turbine_id:
                his_points = his_points.filter(turbine_id=turbine_id)
            if level:
                his_points = his_points.filter(point_type__level=level)
            
            result_list = [get_his_point_serialized_data(hp) for hp in his_points]
            
            return Response({
                "success": True,
                "data": result_list
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error in HISPointListAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class HISPointDetailsView(APIView):
    """API để lấy thông tin chi tiết của một HISPoint"""
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewHISPoint]
    
    def get(self, request, his_point_id=None):
        try:
            if not his_point_id:
                his_point_id = request.query_params.get('his_point_id')
                
            if not his_point_id:
                return Response({
                    "success": False,
                    "error": "HISPoint ID must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy thông tin HISPoint
            try:
                his_point = HISPoint.objects.select_related('farm', 'farm__investor', 'point_type', 'turbine').get(id=his_point_id)
            except HISPoint.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "HISPoint not found",
                    "code": "HISPOINT_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Kiểm tra quyền
            permission_response = check_object_permission(
                request, self, his_point,
                "You don't have permission to access this HISPoint"
            )
            if permission_response:
                return permission_response
            
            return Response({
                "success": True,
                "data": get_his_point_serialized_data(his_point)
            })
            
        except Exception as e:
            logger.error(f"Error in HISPointDetailsView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "PROCESSING_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

