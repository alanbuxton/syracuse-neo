
from api import views
from api.quick_register_views import RegisterAndGetKeyView
from rest_framework import routers
from django.urls import path, re_path

router = routers.DefaultRouter()
router.register(r'geonames', views.GeoNamesViewSet, basename='api-geonameslocation')
router.register(r'regions', views.RegionsViewSet, basename='api-region')
router.register(r'industry_clusters', views.IndustryClusterViewSet, basename='api-industrycluster')
router.register(r'activities', views.ActivitiesViewSet, basename='api-activity' )


# Remove the detail URLs from router and add custom one
detail_patterns = [
    path('activities/<path:uri>/', views.ActivitiesViewSet.as_view({'get': 'retrieve'}), name='api-activity-detail'),
]

urlpatterns = router.urls + detail_patterns +  [
    path('register-and-get-key/', RegisterAndGetKeyView.as_view(), name='register-and-get-key'),
]