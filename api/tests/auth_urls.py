
from rest_framework.decorators import api_view, permission_classes, authentication_classes
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from rest_framework.response import Response
from syracuse.authentication import FlexibleTokenAuthentication
from django.urls import path

# Test views for the endpoints
@api_view(['GET'])
@permission_classes([IsAuthenticated])
@authentication_classes([FlexibleTokenAuthentication])
def protected_view(request):
    return Response({'message': 'You are authenticated!'})


@api_view(['GET'])  
@permission_classes([IsAdminUser])
@authentication_classes([FlexibleTokenAuthentication])
def admin_only_view(request):
    return Response({'message': 'You are an admin!'})


# URL patterns for testing (you'd add these to your test urls.py)
urlpatterns = [
    path('api/protected/', protected_view, name='protected'),
    path('api/admin-only/', admin_only_view, name='admin-only'),
]
