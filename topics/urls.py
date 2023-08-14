from django.urls import path
from topics import views

urlpatterns = [
    path('organizations', views.OrganizationList.as_view(), name='organizations'),
]
