from django.urls import include, path
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView
from api import views
from rest_framework.routers import DefaultRouter


router = DefaultRouter()
router.register(r'usage', views.APIUsageViewSet, basename='api-usage')

urlpatterns = [
    path('api/v1/', include( ("api.v1.urls","v1"),namespace="v1")),
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    # Optional UI:
    path('api/schema/swagger-ui/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/schema/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    path("docs/", SpectacularSwaggerView.as_view(url_name="schema")),
    path('api/', include(router.urls)), 
]
