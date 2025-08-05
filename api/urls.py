from django.urls import include, path
from api import views
from api import no_throttle_views
from rest_framework.routers import DefaultRouter


router = DefaultRouter()
router.register(r'usage', views.APIUsageViewSet, basename='api-usage')

urlpatterns = [
    path('api/v1/', include( ("api.v1.urls","v1"),namespace="v1")),
    path('api/schema/', no_throttle_views.NoThrottleSpectacularAPIView.as_view(), name='schema'),
    # Optional UI:
    path('api/schema/swagger-ui/', no_throttle_views.NoThrottleSpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/schema/redoc/', no_throttle_views.NoThrottleSpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    path("docs/", no_throttle_views.NoThrottleSpectacularSwaggerView.as_view(url_name="schema")),
    path('api/', include(router.urls)), 
]
