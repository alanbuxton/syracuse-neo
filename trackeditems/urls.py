from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# # Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register('v1/activities_by_uri', views.ActivitiesByUriViewSet, basename='api-tracked-activities-by-uri')
router.register('v1/activities_by_industry_region', views.ActivitiesByIndustryRegionViewSet, basename='api-activities-by-industry-region')

urlpatterns = [
    path('api/', include(router.urls)),
    path('tracked_organizations', views.TrackedOrganizationView.as_view(), name='tracked-organizations'),
    path('tracked_industry_geos', views.TrackedIndustryGeoView.as_view(), name='tracked-industry-geos'),
    path('activities', views.ActivitiesView.as_view(), name='tracked-activities'),
    path('geo_activities', views.GeoActivitiesView.as_view(), name='tracked-geo-activities'),
    path('source_activities', views.SourceActivitiesView.as_view(), name='tracked-source-activities'),
    path('activity_stats', views.ActivityStats.as_view(), name="tracked-activity-stats")
]
