"""Farm management views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Farm, Investor
from permissions.views import CanManageFarm, CanViewFarm
from django.db import IntegrityError
from django.db import transaction
from api_gateway.management.farms.validators import validate_farm_data
from api_gateway.management.farms.helpers import get_farm_serialized_data
import logging

logger = logging.getLogger(__name__)

# -------------------------- FARM INFO MANAGEMENT ------------------------
class FarmCreateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageFarm]

    def post(self, request):
        try:
            # Lấy thông tin từ request
            name = request.data.get('name', '').strip()
            address = request.data.get('address', '').strip()
            capacity = request.data.get('capacity', None)
            latitude = request.data.get('latitude', None)
            longitude = request.data.get('longitude', None)
            investor_id = request.data.get('investor_id', None)

            # Validate dữ liệu
            validation_errors = validate_farm_data(name=name, capacity=capacity, latitude=latitude, longitude=longitude)
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Xử lý investor
            investor = None
            
            # Nếu admin chỉ định investor_id
            if request.user.role == "admin" and investor_id:
                try:
                    investor = Investor.objects.get(id=investor_id)
                except Investor.DoesNotExist:
                    return Response({
                        "success": False,
                        "error": f"Investor with ID {investor_id} does not exist",
                        "code": "INVESTOR_NOT_FOUND"
                    }, status=status.HTTP_404_NOT_FOUND)
            
            # Nếu người dùng là investor, tự động gán farm cho họ
            elif request.user.role == "investor":
                if not request.user.investor_profile:
                    return Response({
                        "success": False,
                        "error": "No investor profile found for your account",
                        "code": "NO_INVESTOR_PROFILE"
                    }, status=status.HTTP_403_FORBIDDEN)
                investor = request.user.investor_profile

            # Tạo farm mới
            with transaction.atomic():
                farm = Farm.objects.create(
                    name=name,
                    address=address,
                    capacity=capacity,
                    latitude=latitude,
                    longitude=longitude,
                    investor=investor  
                )
            
            return Response({
                "success": True,
                "message": "Farm created successfully",
                "data": get_farm_serialized_data(farm)
            }, status=status.HTTP_201_CREATED)

        except IntegrityError as e:
            logger.error(f"Database integrity error in FarmCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in FarmCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class FarmUpdateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageFarm]

    def put(self, request, farm_id=None):
        try:
            if not farm_id:
                return Response({
                    "success": False,
                    "error": "Farm ID is required",
                    "code": "MISSING_FARM_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                farm = Farm.objects.get(id=farm_id)
            except Farm.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Farm does not exist",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, farm):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to update this farm",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)

            # Lấy dữ liệu từ request
            name = request.data.get('name', farm.name).strip() if 'name' in request.data else farm.name
            address = request.data.get('address', farm.address).strip() if 'address' in request.data else farm.address
            capacity = request.data.get('capacity', farm.capacity) if 'capacity' in request.data else farm.capacity
            latitude = request.data.get('latitude', farm.latitude) if 'latitude' in request.data else farm.latitude
            longitude = request.data.get('longitude', farm.longitude) if 'longitude' in request.data else farm.longitude

            # Validate dữ liệu
            validation_errors = validate_farm_data(
                name=name if 'name' in request.data else None,
                capacity=capacity if 'capacity' in request.data else None,
                latitude=latitude if 'latitude' in request.data else None,
                longitude=longitude if 'longitude' in request.data else None,
                exclude_farm_id=farm.id
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Update farm
            if 'name' in request.data:
                farm.name = name
            if 'address' in request.data:
                farm.address = address
            if 'capacity' in request.data:
                farm.capacity = capacity
            if 'latitude' in request.data:
                farm.latitude = latitude
            if 'longitude' in request.data:
                farm.longitude = longitude
            farm.save()

            return Response({
                "success": True,
                "message": "Farm updated successfully",
                "data": get_farm_serialized_data(farm)
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in FarmUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in FarmUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class FarmDeleteAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageFarm]

    def delete(self, request, farm_id=None):
        try:
            if not farm_id:
                return Response({
                    "success": False,
                    "error": "Farm ID is required",
                    "code": "MISSING_FARM_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                farm = Farm.objects.get(id=farm_id)
            except Farm.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Farm does not exist",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, farm):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to delete this farm",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)

            farm_name = farm.name
            farm.delete()

            return Response({
                "success": True,
                "message": f"Farm '{farm_name}' has been deleted successfully",
                "data": {
                    "id": farm_id,
                    "name": farm_name
                }
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in FarmDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in FarmDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class FarmListAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            if request.user.role == "admin":
                farms = Farm.objects.select_related("investor").all()

            elif request.user.role == "investor":
                if not request.user.investor_profile:
                    return Response({
                        "success": False,
                        "error": "No investor profile found for your account",
                        "code": "NO_INVESTOR_PROFILE"
                    }, status=status.HTTP_403_FORBIDDEN)
                
                farms = Farm.objects.filter(investor=request.user.investor_profile).select_related("investor")

            elif request.user.role in ["farm_admin", "staff"]:
                if not request.user.farm:
                    return Response({
                        "success": False,
                        "error": "You are not assigned to any farm",
                        "code": "NO_FARM_ASSIGNED"
                    }, status=status.HTTP_403_FORBIDDEN)
                farms = Farm.objects.filter(id=request.user.farm.id).select_related("investor")

            else:
                return Response({
                    "success": False,
                    "error": "You do not have permission",
                    "code": "INVALID_ROLE"
                }, status=status.HTTP_403_FORBIDDEN)
            
            farm_list = [get_farm_serialized_data(farm) for farm in farms]
            
            return Response({
                "success": True,
                "data": farm_list
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error in FarmListAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class FarmDetailsView(APIView):
    """API để lấy thông tin chi tiết của một farm"""
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewFarm]
    
    def get(self, request, farm_id=None):
        try:
            if not farm_id:
                farm_id = request.query_params.get('farm_id')
                
            if not farm_id:
                return Response({
                    "success": False,
                    "error": "Farm ID must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy thông tin farm
            try:
                farm = Farm.objects.select_related('investor').get(id=farm_id)
            except Farm.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Farm not found",
                    "code": "FARM_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, farm):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to access this farm",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)
            
            return Response({
                "success": True,
                "data": get_farm_serialized_data(farm)
            })
            
        except Exception as e:
            logger.error(f"Error in FarmDetailsView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "PROCESSING_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

