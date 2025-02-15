from rest_framework import serializers
from topics.models import Organization, IndustryCluster
import pycountry
from topics.industry_geo import country_admin1_full_name, orgs_by_industry_and_or_geo
from topics.util import elements_from_uri

class RecentsByGeoSerializer(serializers.Serializer):
    def to_representation(self, instance):
        country_code = instance[0]
        repres = {
            "geo_name": instance[1],
            "geo_code": country_code,
            "count7": instance[2],
            "count30": instance[3],
            "count90": instance[4],
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
    
class RecentsByIndustrySerializer(serializers.Serializer):
    def to_representation(self, instance):
        repres = {
            "industry_id": instance[0],
            "industry_name": instance[1],
            "count7": instance[2],
            "count30": instance[3],
            "count90": instance[4],
        }
        return repres     

class CountsSerializer(serializers.Serializer):
    def to_representation(self, instance):
        return {
            "node_type": instance[0],
            "count": instance[1],
        }    

class GeoNamesLocationSerializer(serializers.Serializer):
    uri = serializers.URLField()
    geoNamesURL = serializers.IntegerField
    name = serializers.ListField()

class IndustryClusterSerializer(serializers.Serializer):
    representative_docs = serializers.ListField()
    topic_id = serializers.IntegerField()
    unique_name = serializers.CharField()
    uri = serializers.URLField()

class ActivityActorSerializer(serializers.Serializer):
    name = serializers.ListField()
    best_name = serializers.CharField()
    uri = serializers.URLField()
    industry_as_string = serializers.CharField()
    industry_clusters = IndustryClusterSerializer(many=True)
    based_in_high_geonames_locations = GeoNamesLocationSerializer(many=True)
    based_in_high_clean_names = serializers.ListField()
    based_in_high_as_string = serializers.CharField()

    def to_representation(self, instance):
        repres = super().to_representation(instance)
        repres["uri_parts"] = elements_from_uri(repres["uri"])
        return repres

class ActivitySerializer(serializers.Serializer):
    source_organization = serializers.CharField()
    source_is_core = serializers.BooleanField()
    headline = serializers.CharField()
    date_published = serializers.DateTimeField()
    document_extract = serializers.CharField()
    document_url = serializers.URLField()
    archive_org_page_url = serializers.URLField()
    archive_org_list_url = serializers.URLField()
    activity_uri = serializers.URLField()
    activity_class = serializers.CharField()
    activity_types = serializers.ListField()
    activity_longest_type = serializers.CharField()
    activity_statuses = serializers.ListField()
    activity_status_as_string = serializers.CharField()
    activity_locations = GeoNamesLocationSerializer(many=True)
    activity_location_as_string = serializers.CharField()
    actors = serializers.DictField(
        child = ActivityActorSerializer(many=True)
    )

class OrgIndGeoSerializer(serializers.Serializer):

    def to_representation(self, instance):
        org = Organization.self_or_ultimate_target_node(instance.organization_uri)
        industry_cluster = IndustryCluster.get_by_industry_id(instance.industry_id)

        org_name = 'Any' if org is None else org.best_name
        industry_search_str = 'n/a' if instance.industry_search_str is None else instance.industry_search_str
        if industry_cluster is None:
            industry_name = 'Any' if instance.industry_search_str is None else 'n/a'
        else:
            industry_name = industry_cluster.best_name
        
        region_name = country_admin1_full_name(instance.region)
        if region_name == '':
            region_name = 'Any'

        if industry_cluster is not None or instance.region is not None:
            orgs = orgs_by_industry_and_or_geo(industry_cluster,instance.region)
            org_count = len(orgs)
        else:
            org_count = None

        serialized = {
            "organization": org,
            "industry": industry_cluster,
            "org_name": org_name,
            "industry_name": industry_name,
            "industry_search_str": industry_search_str,
            "region_name": region_name,
            "tracked_item_id": instance.id,
            "uri_parts": elements_from_uri(org.uri) if org else {},
            "geo_code": instance.region or '',
            "org_count": org_count,
        }

        return serialized
    