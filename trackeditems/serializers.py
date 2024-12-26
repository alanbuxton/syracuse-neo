from rest_framework import serializers
from .models import TrackedOrganization, TrackedIndustryGeo
import pycountry
from topics.industry_geo import country_admin1_full_name

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

class TrackedOrganizationSerializer(serializers.Serializer):
    organization_uri = serializers.URLField()
    organization_name = serializers.CharField()

class TrackedOrganizationModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrackedOrganization
        fields = "__all__"

class TrackedIndustryGeoModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrackedIndustryGeo
        fields = "__all__"

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
    

# def industry_geo_search_str(industry, geo):
#     industry_str = "all industries" if industry is None or industry.strip() == '' else industry
#     geo_str = "all locations" if geo is None or geo.strip() == '' else geo
#     if geo_str.split()[0].lower() == 'united':
#         in_str = "in the"
#     else:
#         in_str = "in"
#     return f"<b>{industry_str.title()}</b> {in_str} <b>{geo_str.title()}</b>"

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
