from django.urls import path, register_converter
from topics import views
from .converters import DateConverter

register_converter(DateConverter, 'date')

urlpatterns = [
    path('', views.Index.as_view(), name='index'),
    path('random_organization', views.RandomOrganization.as_view(), name='random-organization'),
    path('organization/uri/<str:domain>/<str:path>/<doc_id>/<str:name>', views.OrganizationByUri.as_view(), name='organization-uri'),
    path('report_issue', views.ReportIssue.as_view(), name='report-issue'),
]
