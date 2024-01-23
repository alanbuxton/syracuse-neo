from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views


# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r"v1/activities", views.ActivitiesViewSet, basename='api-tracked-activities')
router.register(r"v1/geo_activities", views.GeoActivitiesViewSet, basename='api-tracked-geo-activities')
router.register(r"v1/source_activities", views.SourceActivitiesViewSet, basename='api-tracked-source-activities')

urlpatterns = [
    path('api/', include(router.urls)),
    path('tracked_organizations', views.TrackedOrganizationView.as_view(), name='tracked-organizations'),
    path('activities', views.ActivitiesView.as_view(), name='tracked-activities'),
    path('geo_activities', views.GeoActivitiesView.as_view(), name='tracked-geo-activities'),
    path('source_activities', views.SourceActivitiesView.as_view(), name='tracked-source-activities'),
    path('activity_stats', views.ActivityStats.as_view(), name="tracked-activity-stats")
]
