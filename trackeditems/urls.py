from django.urls import path
from . import views

urlpatterns = [
    path('tracked_organizations', views.TrackedOrganizationView.as_view(), name='tracked-organizations'),
    path('tracked_industry_geos', views.TrackedIndustryGeoView.as_view(), name='tracked-industry-geos'),
    path('activities', views.ActivitiesView.as_view(), name='tracked-activities'),
    path('geo_activities', views.GeoActivitiesView.as_view(), name='tracked-geo-activities'),
    path('source_activities', views.SourceActivitiesView.as_view(), name='tracked-source-activities'),
    path('activity_stats', views.ActivityStats.as_view(), name="tracked-activity-stats"),
    path('industry_activities', views.IndustryActivitiesView.as_view(), name="industry-activities"),
]
