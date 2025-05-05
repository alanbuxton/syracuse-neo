from rest_framework import serializers
from django.urls import NoReverseMatch
from rest_framework.reverse import reverse
from .fields import HyperlinkedRelationshipField
import logging
from topics.util import camel_case_to_snake_case

logger = logging.getLogger(__name__)

class HyperlinkedNeomodelSerializer(serializers.Serializer):

    attribs_to_ignore = []
    single_rels = []
    use_internal_id_as_id = True

    def mapped_name(self, field_name):
        name = camel_case_to_snake_case(field_name)
        return name

    def do_not_serialize(self):
        return self.context.get("attribs_to_ignore",self.attribs_to_ignore)

    def to_representation(self, instance):
        data = {}
        # Include all properties
        for prop, _ in instance.__all_properties__:
            if prop in self.do_not_serialize():
                continue
            data[self.mapped_name(prop)] = getattr(instance, prop, None)
    
        if self.use_internal_id_as_id is True:
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
                assert len(related_nodes) <= 1, f"Expected {rel_name} on {instance.uri} to have zero or one relationships"
                related_nodes = related_nodes[0] if len(related_nodes) == 1 else None
                many = False

            if related_nodes is None:
                data[self.mapped_name(rel_name)] = None
            else:
                try:
                    field = HyperlinkedRelationshipField(
                        view_name=view_name,
                        many=many,
                    )
                    field.bind(field_name=rel_name, parent=self)  # Inject context
                    data[self.mapped_name(rel_name)] = field.to_representation(related_nodes)
                except NoReverseMatch:
                    data[self.mapped_name(rel_name)] = [x.uri  for x in related_nodes]
        return data

class IndustryClusterSerializer(HyperlinkedNeomodelSerializer):
    single_rels = ["parentLeft","parentRight","childLeft","childRight"]
    attribs_to_ignore = ["foundName","name","internalDocId","internalId",
                         "internalMergedSameAsHighToUri","documentSource","sameAsHigh",
                         "orgsPrimary","orgsSecondary","peoplePrimary","peopleSecondary"]
    

class RegionsDictSerializer(serializers.Serializer):
    id = serializers.CharField()
    parent = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()

    def get_parent(self, obj):
        request = self.context.get('request')
        field = obj['parent']
        if field is None:
            return None
        return reverse('api-region-detail', kwargs={'pk': obj['parent']}, request=request)
    
    def get_children(self, obj):
        request = self.context.get('request')
        return [
            reverse('api-region-detail', kwargs={'pk': rid}, request=request)
            for rid in obj.get('children', [])
        ]

class GeoNamesSerializer(serializers.Serializer):
    geonames_id = serializers.IntegerField(source="geoNamesId")
    uri = serializers.URLField() # Unique URI within 1145
    name = serializers.SerializerMethodField()
    geonames_url = serializers.URLField(source="geoNamesURL")
    country_code = serializers.CharField(source="countryCode")
    admin1_code =serializers.SerializerMethodField()
    region = serializers.SerializerMethodField()

    def get_name(self, obj):
        name = obj.name
        name = name[0] if isinstance(name, list) and len(name) > 0 else ''
        return name
    
    def get_admin1_code(self, obj):
        if obj.admin1Code == '00':
            return None
        return obj.admin1Code
    
    def get_region(self, obj):
        admin1 = self.get_admin1_code(obj)
        region_code = obj.countryCode
        if admin1 is not None:
            region_code = f"{region_code}-{admin1}"
        request = self.context.get('request')
        target = reverse('api-region-detail', kwargs={'pk': region_code}, request=request)
        return target
    
class ShortIndustryClusterSerializer(serializers.Serializer):
    uri = serializers.URLField() # Unique URI
    representative_docs = serializers.ListField(source="representativeDoc")
    details = serializers.SerializerMethodField() # Link to API

    def get_details(self, obj):
        request = self.context.get('request')
        target = reverse('api-industrycluster-detail', kwargs={'pk': obj.pk}, request=request)
        return target


class ActivityActorSerializer(serializers.Serializer):
    name = serializers.CharField(source="best_name")
    uri = serializers.URLField()
    based_in = GeoNamesSerializer(many=True,source="basedInHighGeoNamesLocation")
    industries = ShortIndustryClusterSerializer(many=True,source="industryClusterPrimary",required=False)


class ActivitySerializer(serializers.Serializer):
    activity_class = serializers.CharField()
    headline = serializers.CharField()
    date_published = serializers.DateTimeField()
    source_organization = serializers.CharField()
    document_extract = serializers.CharField()
    document_url = serializers.URLField()
    activity_uri = serializers.URLField()
    activity_locations = GeoNamesSerializer(many=True)
    actors = serializers.DictField(
        child = ActivityActorSerializer(many=True)
    )
    archive_org_list_url = serializers.URLField()
