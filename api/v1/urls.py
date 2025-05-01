
from api import views
from rest_framework import routers

router = routers.DefaultRouter()
router.register(r'geos', views.GeosViewSet, basename='api-geos')
router.register(r'industry_clusters', views.IndustryClusterViewSet, basename='api-industrycluster')

urlpatterns = router.urls
