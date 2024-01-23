from rest_framework import serializers
from .models import TrackedOrganization
from topics.models import Organization
from django.contrib.auth.models import  User
import pycountry

class RecentsByGeoSerializer(serializers.Serializer):
    def to_representation(self, instance):
        country_code = instance[0]
        repres = {
            "geo_name": instance[2],
            "country_code": country_code,
            "geo_code": instance[1],
            "count7": instance[3],
            "count30": instance[4],
            "count90": instance[5],
        }
        pyc = pycountry.countries.get(alpha_2=country_code)
        if pyc and pyc.flag:
            repres["flag"] = pyc.flag
        return repres

class RecentsBySourceSerializer(serializers.Serializer):
    def to_representation(self, instance):
        repres = {
            "source_name": instance[0],
            "count7": instance[1],
            "count30": instance[2],
            "count90": instance[3],
        }
        return repres

class CountsSerializer(serializers.Serializer):
    def to_representation(self, instance):
        return {
            "node_type": instance[0],
            "count": instance[1],
        }

class TrackedOrganizationSerializer(serializers.Serializer):
    organization_uri = serializers.URLField()
    organization_name = serializers.CharField()

class TrackedOrganizationModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrackedOrganization
        fields = "__all__"

class ActivityParticipantSerializer(serializers.Serializer):
    name = serializers.ListField()
    longest_name = serializers.CharField()
    uri = serializers.URLField()

class ActivitySerializer(serializers.Serializer):
    source_name = serializers.CharField()
    document_date = serializers.DateTimeField()
    document_extract = serializers.CharField()
    document_title = serializers.CharField()
    document_url = serializers.URLField()
    activity_uri = serializers.URLField()
    activity_class = serializers.CharField()
    activity_types = serializers.ListField()
    activity_longest_type = serializers.CharField()
    activity_statuses = serializers.ListField()
    activity_status_as_string = serializers.CharField()
    participants = serializers.DictField(
        child=ActivityParticipantSerializer(many=True)
    )
