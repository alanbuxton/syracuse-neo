from rest_framework import serializers
from topics.models import Organization

class OrganizationSerializer(serializers.BaseSerializer):

    def to_representation(self,instance):
        data = {
            "uri": instance.uri,
            "name": instance.name,
            "industry": instance.industry,
            "description": instance.description,
        }
        return data
