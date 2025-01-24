from rest_framework import serializers
from .models import TrackedOrganization, TrackedIndustryGeo
from topics.models import Organization, IndustryCluster
import pycountry
from topics.industry_geo import country_admin1_full_name

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

class TrackedIndustryGeoSerializer(serializers.Serializer):
    
    def to_representation(self, instance):
        geo_code = instance.geo_code
        ind = instance.industry_name
        region_str = "All Locations" if (geo_code is None or 
                                          geo_code.strip() == '') else country_admin1_full_name(geo_code)
        industry_str = "All Industries" if (ind is None or ind.strip() == '') else ind
        in_str = "in the" if region_str.lower().startswith("united") else "in"
        return {"geo_code":geo_code,
                "region_str": region_str,
                "industry_name": ind,
                "industry_str": industry_str,
                "in_str": in_str,
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

        serialized = {
            "organization": org,
            "industry": industry_cluster,
            "org_name": org_name,
            "industry_name": industry_name,
            "industry_search_str": industry_search_str,
            "region_name": region_name,
            "tracked_item_id": instance.id,
        }

        return serialized
    