from rest_framework import serializers
from .models import TrackedOrganization
from django.contrib.auth.models import  User

class TrackedOrganizationSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrackedOrganization
        fields = '__all__'

    def upsert(self, validated_data):
        existing_object = TrackedOrganization.get_case_insensitive(validated_data)
        if existing_object is None:
            instance = self.create(validated_data)
        else:
            instance = self.update(existing_object, validated_data)
        return instance


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
