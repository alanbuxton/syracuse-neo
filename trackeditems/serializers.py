from rest_framework import serializers
from .models import TrackedOrganization
from topics.models import Organization
from django.contrib.auth.models import  User

class TrackedOrganizationSerializer(serializers.Serializer):
    organization_uri = serializers.URLField()
    organization_name = serializers.CharField()

class TrackedOrganizationModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrackedOrganization
        fields = "__all__"

class ActivitySerializer(serializers.Serializer):
    source_name = serializers.CharField()
    document_date = serializers.DateTimeField()
    document_extract = serializers.CharField()
    document_title = serializers.CharField()
    document_url = serializers.URLField()
    organization_names = serializers.ListField()
    organization_longest_name = serializers.CharField()
    organization_uri = serializers.URLField()
    activity_uri = serializers.URLField()
    activity_class = serializers.CharField()
    activity_types = serializers.ListField()
    activity_longest_type = serializers.CharField()
    activity_statuses = serializers.ListField()
    activity_status_as_string = serializers.CharField()
