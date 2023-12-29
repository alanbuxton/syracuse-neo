from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views


# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r"v1/activities", views.ActivitiesViewSet, basename='tracked-activities')

urlpatterns = [
    path('api/', include(router.urls)),
    path('tracked_organizations', views.TrackedOrganizationView.as_view(), name='tracked-organizations'),
    path('recent_activities', views.ActivitiesView.as_view(), name='recent-activities'),
]
