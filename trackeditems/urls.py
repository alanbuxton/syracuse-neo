from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views


# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r"v1/activities", views.ActivitiesViewSet, basename='api-tracked-activities')
router.register(r"v1/country_activities", views.CountryActivitiesViewSet, basename='api-tracked-country-activities')

urlpatterns = [
    path('api/', include(router.urls)),
    path('tracked_organizations', views.TrackedOrganizationView.as_view(), name='tracked-organizations'),
    path('activities', views.ActivitiesView.as_view(), name='tracked-activities'),
    path('country_activities', views.CountryActivitiesView.as_view(), name='tracked-country-activities'),
    path('activity_stats', views.ActivityStats.as_view(), name="tracked-activity-stats")
]
