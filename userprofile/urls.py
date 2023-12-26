from django.urls import path
from userprofile import views

urlpatterns = [
    path('tracked_organizations', views.TrackedOrganizationView.as_view(), name='tracked-organizations'),
]
