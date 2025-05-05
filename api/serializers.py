from rest_framework import serializers
from django.urls import NoReverseMatch
from rest_framework.reverse import reverse
from .fields import HyperlinkedRelationshipField
import logging

logger = logging.getLogger(__name__)

class HyperlinkedNeomodelSerializer(serializers.Serializer):

    attribs_to_ignore = []
    single_rels = []

    def do_not_serialize(self):
        return self.context.get("attribs_to_ignore",self.attribs_to_ignore)

    def to_representation(self, instance):
        data = {}
        # Include all properties
        for prop, _ in instance.__all_properties__:
            if prop in self.do_not_serialize():
                continue
            data[prop] = getattr(instance, prop, None)
        data['id'] = instance.internalId

        for rel_name, rel_obj in instance.__all_relationships__:
            if rel_name in self.do_not_serialize():
                continue
            node_class = rel_obj.definition.get("node_class", None)
            if not node_class:
                continue
            view_name = f"api-{node_class.__name__.lower()}-detail"

            related_nodes = getattr(instance, rel_name)
            many = True
            if rel_name in self.single_rels:
                assert len(related_nodes) <= 1, f"Expecte {rel_name} on {instance.uri} to have zero or one relationships"
                related_nodes = related_nodes[0] if len(related_nodes) == 1 else None
                many = False

            if related_nodes is None:
                data[rel_name] = None
            else:
                try:
                    field = HyperlinkedRelationshipField(
                        view_name=view_name,
                        many=many,
                    )
                    field.bind(field_name=rel_name, parent=self)  # Inject context
                    data[rel_name] = field.to_representation(related_nodes)
                except NoReverseMatch:
                    data[rel_name] = [x.uri  for x in related_nodes]
        return data

class IndustryClusterSerializer(HyperlinkedNeomodelSerializer):
    single_rels = ["parentLeft","parentRight","childLeft","childRight"]
    attribs_to_ignore = ["foundName","name","internalDocId","internalId",
                         "internalMergedSameAsHighToUri","documentSource","sameAsHigh",
                         "orgsPrimary","orgsSecondary","peoplePrimary","peopleSecondary"]

class GeoDictSerializer(serializers.Serializer):
    id = serializers.CharField()
    parent = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()

    def get_parent(self, obj):
        request = self.context.get('request')
        field = obj['parent']
        if field is None:
            return None
        return reverse('api-geos-detail', kwargs={'pk': obj['parent']}, request=request)
    
    def get_children(self, obj):
        request = self.context.get('request')
        return [
            reverse('api-geos-detail', kwargs={'pk': rid}, request=request)
            for rid in obj.get('children', [])
        ]


class GeoNamesLocationSerializer(serializers.Serializer):
    uri = serializers.URLField()
    geoNamesURL = serializers.URLField()
    name = serializers.SerializerMethodField()

    def get_name(self, obj):
        name = obj.name
        name = name[0] if isinstance(name, list) and len(name) > 0 else ''
        return name


class ActivityActorSerializer(serializers.Serializer):
    name = serializers.CharField(source="best_name")
    uri = serializers.URLField()
    based_in_high_geonames_locations = GeoNamesLocationSerializer(many=True)
    industries = serializers.SerializerMethodField()

    def get_industries(self, obj):
        request = self.context.get('request')
        try:
            return [
                reverse('api-industrycluster-detail', kwargs={'pk': rid.topicId}, request=request)
                for rid in obj.industryClusterPrimary
            ]
        except AttributeError: # Might not have industryClusterPrimary
            return []

class ActivitySerializer(serializers.Serializer):
    activity_class = serializers.CharField()
    headline = serializers.CharField()
    date_published = serializers.DateTimeField()
    source_organization = serializers.CharField()
    document_extract = serializers.CharField()
    document_url = serializers.URLField()
    activity_uri = serializers.URLField()
    activity_locations = GeoNamesLocationSerializer(many=True)
    actors = serializers.DictField(
        child = ActivityActorSerializer(many=True)
    )
    archive_org_list_url = serializers.URLField()
