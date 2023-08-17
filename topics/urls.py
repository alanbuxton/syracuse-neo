from django.urls import path
from topics import views

urlpatterns = [
    path('', views.Index.as_view(), name='index'),
    path('random_organization', views.RandomOrganization.as_view(), name='random-organization'),
    path('organization/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationByUri.as_view(), name='organization-uri'),

]
