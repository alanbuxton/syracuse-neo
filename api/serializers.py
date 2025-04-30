from django.contrib.auth.models import Group, User
from rest_framework import serializers
from django.urls import reverse, NoReverseMatch
from .fields import HyperlinkedRelationshipField
import logging

logger = logging.getLogger(__name__)

class HyperlinkedNeomodelSerializer(serializers.Serializer):
    def to_representation(self, instance):
        data = {}

        # Include all properties
        for prop, _ in instance.__all_properties__:
            if prop in self.context["attribs_to_ignore"]:
                continue
            data[prop] = getattr(instance, prop, None)
        data['id'] = instance.internalId

        for rel_name, rel_obj in instance.__all_relationships__:
            if rel_name in self.context["attribs_to_ignore"]:
                continue
            node_class = rel_obj.definition.get("node_class", None)
            if not node_class:
                continue
            related_nodes = getattr(instance, rel_name)
            view_name = f"api-{node_class.__name__.lower()}-detail"
            try:
                field = HyperlinkedRelationshipField(
                    view_name=view_name,
                    many=True,
                )
                field.bind(field_name=rel_name, parent=self)  # Inject context
                data[rel_name] = field.to_representation(related_nodes)
            except NoReverseMatch:
                data[rel_name] = [x.uri for x in related_nodes]

        return data
