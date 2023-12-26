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
