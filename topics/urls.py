from django.urls import path, register_converter, include
from topics import views
# from .converters import DateConverter
# from rest_framework.routers import DefaultRouter

# register_converter(DateConverter, 'date')

# router = DefaultRouter(trailing_slash=False)
# router.register("v1/industries", views.IndustriesViewSet, basename='api-industry-list')
# router.register("v1/countries_regions", views.CountriesAndRegionsViewSet, basename='api-geo-list')

urlpatterns = [
    # path('api/', include(router.urls)),
    path('', views.Index.as_view(), name='index'),
    path('organization/linkages/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationByUri.as_view(), name='organization-linkages'),
    path('organization/timeline/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationTimeline.as_view(), name='organization-timeline'),
    path('organization/family-tree/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.FamilyTree.as_view(), name='organization-family-tree'),
    path('about', views.About.as_view(), name="about"),
    path('resource/<str:domain>/<str:path>/<doc_id>/<str:name>', views.ShowResource.as_view(), name='resource-with-doc-id'),
    path('resource/<str:domain>/<str:path>/<str:name>', views.ShowResource.as_view(), name='resource-no-doc-id'),
    path('industry_geo_finder', views.IndustryGeoFinder.as_view(), name='industry-geo-finder'),
    path('industry_geo_finder_review', views.IndustryGeoFinderReview.as_view(), name='industry-geo-finder-review'),
]
