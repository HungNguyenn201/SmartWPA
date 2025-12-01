"""Turbine management views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from facilities.models import Farm, Turbines
from permissions.views import CanManageTurbine, CanViewTurbine
from django.db import IntegrityError
from django.db import transaction
from api_gateway.management.turbines.validators import validate_turbine_data
from api_gateway.management.turbines.helpers import get_turbine_serialized_data
import logging

logger = logging.getLogger(__name__)

# -------------------------- TURBINES INFO MANAGEMENT ------------------------
class TurbineCreateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageTurbine]

    def post(self, request, farm_id=None):
        try:
            # Lấy farm_id từ URL hoặc request data
            if not farm_id:
                farm_id = request.data.get('farm_id')
            
            name = request.data.get('name', '').strip()
            capacity = request.data.get('capacity', None)
            latitude = request.data.get('latitude', None)
            longitude = request.data.get('longitude', None)

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

            # Kiểm tra quyền - permission class đã kiểm tra, nhưng kiểm tra lại để chắc chắn
            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    # Tạo một turbine tạm để kiểm tra quyền
                    temp_turbine = Turbines(farm=farm)
                    if not permission.has_object_permission(request, self, temp_turbine):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to create turbines for this farm",
                            "code": "FARM_ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)

            # Validate dữ liệu
            validation_errors = validate_turbine_data(
                name=name,
                capacity=capacity,
                latitude=latitude,
                longitude=longitude,
                farm=farm
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Tạo turbine mới
            with transaction.atomic():
                turbine = Turbines.objects.create(
                    name=name,
                    farm=farm,
                    capacity=capacity,
                    latitude=latitude,
                    longitude=longitude
                )
            
            return Response({
                "success": True,
                "message": "Turbine created successfully",
                "data": get_turbine_serialized_data(turbine)
            }, status=status.HTTP_201_CREATED)

        except IntegrityError as e:
            logger.error(f"Database integrity error in TurbineCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in TurbineCreateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class TurbineUpdateAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageTurbine]

    def put(self, request, turbine_id=None):
        try:
            if not turbine_id:
                return Response({
                    "success": False,
                    "error": "Turbine ID is required",
                    "code": "MISSING_TURBINE_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                turbine = Turbines.objects.get(id=turbine_id)
            except Turbines.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Turbine does not exist",
                    "code": "TURBINE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền - permission class đã kiểm tra, nhưng kiểm tra lại để chắc chắn
            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, turbine):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to update this turbine",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)

            # Lấy dữ liệu từ request
            name = request.data.get('name', turbine.name).strip() if 'name' in request.data else turbine.name
            capacity = request.data.get('capacity', turbine.capacity) if 'capacity' in request.data else turbine.capacity
            latitude = request.data.get('latitude', turbine.latitude) if 'latitude' in request.data else turbine.latitude
            longitude = request.data.get('longitude', turbine.longitude) if 'longitude' in request.data else turbine.longitude
            is_active = request.data.get('is_active', turbine.is_active) if 'is_active' in request.data else turbine.is_active

            # Validate dữ liệu
            validation_errors = validate_turbine_data(
                name=name if 'name' in request.data else None,
                capacity=capacity if 'capacity' in request.data else None,
                latitude=latitude if 'latitude' in request.data else None,
                longitude=longitude if 'longitude' in request.data else None,
                farm=turbine.farm,
                exclude_turbine_id=turbine.id
            )
            if validation_errors:
                return Response({
                    "success": False,
                    "error": validation_errors[0]["error"],
                    "code": validation_errors[0]["code"]
                }, status=status.HTTP_400_BAD_REQUEST)

            # Update turbine
            if 'name' in request.data:
                turbine.name = name
            if 'capacity' in request.data:
                turbine.capacity = capacity
            if 'latitude' in request.data:
                turbine.latitude = latitude
            if 'longitude' in request.data:
                turbine.longitude = longitude
            if 'is_active' in request.data:
                turbine.is_active = is_active
            turbine.save()

            return Response({
                "success": True,
                "message": "Turbine updated successfully",
                "data": get_turbine_serialized_data(turbine)
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in TurbineUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in TurbineUpdateAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class TurbineDeleteAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanManageTurbine]

    def delete(self, request, turbine_id=None):
        try:
            if not turbine_id:
                return Response({
                    "success": False,
                    "error": "Turbine ID is required",
                    "code": "MISSING_TURBINE_ID"
                }, status=status.HTTP_400_BAD_REQUEST)

            try:
                turbine = Turbines.objects.get(id=turbine_id)
            except Turbines.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Turbine does not exist",
                    "code": "TURBINE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)

            # Kiểm tra quyền - permission class đã kiểm tra, nhưng kiểm tra lại để chắc chắn
            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, turbine):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to delete this turbine",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)

            turbine_name = turbine.name
            turbine.delete()

            return Response({
                "success": True,
                "message": f"Turbine '{turbine_name}' has been deleted successfully",
                "data": {
                    "id": turbine_id,
                    "name": turbine_name
                }
            }, status=status.HTTP_200_OK)

        except IntegrityError as e:
            logger.error(f"Database integrity error in TurbineDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "Database integrity error occurred",
                "code": "DATABASE_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            logger.error(f"Unexpected error in TurbineDeleteAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class TurbineListAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            if request.user.role == "admin":
                turbines = Turbines.objects.select_related("farm", "farm__investor").all()

            elif request.user.role == "investor":
                if not request.user.investor_profile:
                    return Response({
                        "success": False,
                        "error": "No investor profile found for your account",
                        "code": "NO_INVESTOR_PROFILE"
                    }, status=status.HTTP_403_FORBIDDEN)
                    
                # Lấy tất cả farm của investor
                farms = Farm.objects.filter(investor=request.user.investor_profile)
                turbines = Turbines.objects.filter(farm__in=farms).select_related('farm', 'farm__investor')

            elif request.user.role in ["farm_admin", "staff"]:
                if not request.user.farm:
                    return Response({
                        "success": False,
                        "error": "You are not assigned to any farm",
                        "code": "NO_FARM_ASSIGNED"
                    }, status=status.HTTP_403_FORBIDDEN)
                turbines = Turbines.objects.filter(farm=request.user.farm).select_related("farm", "farm__investor")

            else:
                return Response({
                    "success": False,
                    "error": "You do not have permission",
                    "code": "INVALID_ROLE"
                }, status=status.HTTP_403_FORBIDDEN)
            
            turbine_list = [get_turbine_serialized_data(turbine) for turbine in turbines]
            
            return Response({
                "success": True,
                "data": turbine_list
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error in TurbineListAPIView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "INTERNAL_SERVER_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class TurbineDetailsView(APIView):
    """API để lấy thông tin chi tiết của một turbine"""
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated, CanViewTurbine]
    
    def get(self, request, turbine_id=None):
        try:
            if not turbine_id:
                turbine_id = request.query_params.get('turbine_id')
                
            if not turbine_id:
                return Response({
                    "success": False,
                    "error": "Turbine ID must be specified",
                    "code": "MISSING_PARAMETERS"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Lấy thông tin turbine
            try:
                turbine = Turbines.objects.select_related('farm', 'farm__investor').get(id=turbine_id)
            except Turbines.DoesNotExist:
                return Response({
                    "success": False,
                    "error": "Turbine not found",
                    "code": "TURBINE_NOT_FOUND"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Kiểm tra quyền - permission class đã kiểm tra, nhưng kiểm tra lại để chắc chắn
            for permission in self.get_permissions():
                if hasattr(permission, 'has_object_permission'):
                    if not permission.has_object_permission(request, self, turbine):
                        return Response({
                            "success": False,
                            "error": "You don't have permission to access this turbine",
                            "code": "ACCESS_DENIED"
                        }, status=status.HTTP_403_FORBIDDEN)
            
            return Response({
                "success": True,
                "data": get_turbine_serialized_data(turbine)
            })
            
        except Exception as e:
            logger.error(f"Error in TurbineDetailsView: {str(e)}")
            return Response({
                "success": False,
                "error": "An unexpected error occurred",
                "code": "PROCESSING_ERROR"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

