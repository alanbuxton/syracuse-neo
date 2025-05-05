
from api import views
from rest_framework import routers

router = routers.DefaultRouter()
router.register(r'geonames', views.GeoNamesViewSet, basename='api-geonameslocation')
router.register(r'regions', views.RegionsViewSet, basename='api-region')
router.register(r'industry_clusters', views.IndustryClusterViewSet, basename='api-industrycluster')
router.register(r'activities', views.ActivitiesViewSet, basename='api-activity' )

urlpatterns = router.urls
