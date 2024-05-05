from django.urls import path, register_converter
from topics import views
from .converters import DateConverter

register_converter(DateConverter, 'date')

urlpatterns = [
    path('', views.Index.as_view(), name='index'),
    path('random_organization', views.RandomOrganization.as_view(), name='random-organization'),
    path('organization/linkages/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationByUri.as_view(), name='organization-linkages'),
    path('organization/timeline/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationTimeline.as_view(), name='organization-timeline'),
    path('organization/children/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationChildrenList.as_view(), name='organization-children-list'),
    path('timeline', views.TopicsTimeline.as_view(), name="timeline"),
    path('parent_child',views.ParentChildWithSearch.as_view(), name="parent-child"),
    path('about', views.About.as_view(), name="about"),
    path('resource/<str:domain>/<str:path>/<doc_id>/<str:name>', views.ShowResource.as_view(), name='resource-with-doc-id'),
    path('resource/<str:domain>/<str:path>/<str:name>', views.ShowResource.as_view(), name='resource-no-doc-id'),
]
