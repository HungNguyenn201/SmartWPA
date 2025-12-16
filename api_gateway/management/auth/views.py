"""Authentication views"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.permissions import IsAuthenticated
from permissions.models import Account
from datetime import datetime
from .helpers import get_token_for_user, check_license
import logging

logger = logging.getLogger(__name__)
class TokenRefreshView(APIView):
    def post(self, request, format=None):
        refresh_token = request.data.get('refresh')
        if not refresh_token:
            return Response({
                'success': False,
                'error': 'Refresh token is missing'}, status=status.HTTP_400_BAD_REQUEST)
        try:
            refresh = RefreshToken(refresh_token)
            refresh.verify()
            # get info user from token
            user_id = refresh.payload.get('user_id')
            user = Account.objects.get(id=user_id)

            if not user.is_active:
                return Response({
                    'success': False,
                    'error': 'User account is not active'}, status=status.HTTP_403_FORBIDDEN)
                    
            # Kiểm tra license mỗi khi refresh token
            if user.role in ['investor', 'farm_admin', 'staff']:
                if not check_license(user):
                    if user.role == 'investor':
                        error_message = 'License is invalid or expired'
                    else:
                        error_message = 'Farm license is invalid or expired'
                    return Response({
                        'success': False,
                        'error': error_message}, status=status.HTTP_403_FORBIDDEN)
                        
            access_token = refresh.access_token
            exp_timestamp = access_token['exp']
            exp_datetime = datetime.utcfromtimestamp(exp_timestamp).strftime('%Y-%m-%d %H:%M:%S')

            return Response({
                'success': True,
                'data': {
                    'token': {'access': str(access_token)},
                    'expires_at': exp_datetime
                }
            }, status=status.HTTP_200_OK)
        except Account.DoesNotExist:
            return Response({
                'success': False,
                'error': 'User does not exist'}, status=status.HTTP_404_NOT_FOUND)
        except Exception:
            return Response({
                'success': False,
                'error': 'Invalid token'}, status=status.HTTP_400_BAD_REQUEST)

class UserLoginView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request, format=None):
        username = request.data.get('username')
        password = request.data.get('password')

        if not username or not password:
            return Response(
                {'success': False, 'error': 'Username and password are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            user = Account.objects.get(username=username)
        except Account.DoesNotExist:
            return Response(
                {'success': False, 'error': 'Invalid username or password'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if not user.is_active:
            return Response(
                {'success': False, 'error': 'Your account has been deactivated. Please contact support.'},
                status=status.HTTP_403_FORBIDDEN
            )
        if not user.check_password(password):
            return Response(
                {'success': False, 'error': 'Invalid username or password'},
                status=status.HTTP_400_BAD_REQUEST
            )
        if user.role == 'investor':
            if not check_license(user):
                return Response(
                    {'success': False, 'error': 'License is invalid or expired'},
                    status=status.HTTP_403_FORBIDDEN
                )
        elif user.role in ['farm_admin', 'staff'] and user.farm:
            if not check_license(user):
                return Response(
                    {'success': False, 'error': 'Farm license is invalid or expired'},
                    status=status.HTTP_403_FORBIDDEN
                )
        token = get_token_for_user(user)
        return Response({
            'success': True,
            'data': {
                'token': token,
                'username': user.username,
                'role': user.role
            }
        }, status=status.HTTP_200_OK)

class LogoutAPIView(APIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            refresh_token = request.data.get('refresh_token')
            if not refresh_token:
                return Response({
                    "success": False,
                    "error": "Refresh token is required",
                    "code": "REFRESH_TOKEN_REQUIRED"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                token = RefreshToken(refresh_token)
                token.blacklist()
            except Exception as token_error:
                logger.warning(f"Error blacklisting token: {str(token_error)}")
                return Response({
                    "success": False,
                    "error": "Failed to invalidate token",
                    "code": "TOKEN_BLACKLIST_FAILED"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            return Response({
                "success": True,
                "message": "Logged out successfully"
            }, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Logout error: {str(e)}")
            return Response({
                "success": False,
                "error": str(e),
                "code": "LOGOUT_FAILED"
            }, status=status.HTTP_400_BAD_REQUEST)

