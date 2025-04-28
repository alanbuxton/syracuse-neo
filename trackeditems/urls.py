from django.urls import path
from . import views


urlpatterns = [
    path('activities', views.ActivitiesView.as_view(), name='tracked-activities'),
    path('geo_activities', views.GeoActivitiesView.as_view(), name='tracked-geo-activities'),
    path('source_activities', views.SourceActivitiesView.as_view(), name='tracked-source-activities'),
    path('activity_stats', views.ActivityStats.as_view(), name="tracked-activity-stats"),
    path('industry_activities', views.IndustryActivitiesView.as_view(), name="industry-activities"),
    path('tracked_org_ind_geo', views.TrackedOrgIndGeoView.as_view(), name="tracked-org-ind-geo"),
    path('industry_geo_activities', views.IndustryGeoActivitiesView.as_view(), name='industry-geo-activities'),
    path("toggle_similar_organizations/<int:item_id>/", views.ToggleTrackedItemView.as_view(), name="toggle-similar-organizations"),
]
