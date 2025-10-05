from rest_framework import serializers
from django.urls import NoReverseMatch
from rest_framework.reverse import reverse
from .fields import HyperlinkedRelationshipField
import logging
from topics.util import camel_case_to_snake_case
from drf_spectacular.utils import extend_schema_field
from api.docstrings import activity_docstring_markdown

logger = logging.getLogger(__name__)

class HyperlinkedNeomodelSerializer(serializers.Serializer):
    # Base serializer for neomodel instances
    attribs_to_ignore = []
    single_rels = []
    use_internal_id_as_id = True

    def mapped_name(self, field_name):
        return camel_case_to_snake_case(field_name)

    def do_not_serialize(self):
        return self.context.get("attribs_to_ignore", self.attribs_to_ignore)

    def to_representation(self, instance):
        data = {}
        for prop, _ in instance.__all_properties__:
            if prop in self.do_not_serialize():
                continue
            data[self.mapped_name(prop)] = getattr(instance, prop, None)
    
        if self.use_internal_id_as_id:
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
                assert len(related_nodes) <= 1
                related_nodes = related_nodes[0] if related_nodes else None
                many = False
                
            try:
                field = HyperlinkedRelationshipField(view_name=view_name, many=many)
                field.bind(field_name=rel_name, parent=self)
                data[self.mapped_name(rel_name)] = field.to_representation(related_nodes)
            except NoReverseMatch:
                data[self.mapped_name(rel_name)] = (
                    None if related_nodes is None else [x.uri for x in related_nodes]
                )
        return data


class IndustryClusterSerializer(HyperlinkedNeomodelSerializer):
    # Add explicit fields so drf-spectacular can see them
    uri = serializers.URLField(read_only=True, help_text = "Unique URI for this industry cluster within the 1145 namespace")
    representation = serializers.ListField(child=serializers.CharField(), help_text="List of representative words")
    representative_doc = serializers.ListField(child=serializers.CharField(), help_text="List of representative docs")
    topic_id = serializers.IntegerField(read_only=True, help_text="Unique ID for this industry, also referred to as industry_id elsewhere")
    child_left = serializers.URLField(read_only=True, allow_null=True, help_text="URI for child industry within 1145 namespace (if available)")
    child_right = serializers.URLField(read_only=True, allow_null=True, help_text="URI for child industry within 1145 namespace (if available)")
    parent_left = serializers.URLField(read_only=True, allow_null=True, help_text="URI for child industry within 1145 namespace (if available)")
    parent_right = serializers.URLField(read_only=True, allow_null=True, help_text="URI for child industry within 1145 namespace (if available)")
    

    single_rels = ["parentLeft", "parentRight", "childLeft", "childRight"]
    attribs_to_ignore = [
        "foundName", "name", "internalDocId", "internalId",
        "internalMergedSameAsHighToUri", "documentSource", "sameAsHigh",
        "orgsPrimary", "orgsSecondary", "peoplePrimary", "peopleSecondary"
    ]


class RegionsDictSerializer(serializers.Serializer):
    id = serializers.CharField(help_text="Region ID or code")
    parent = serializers.SerializerMethodField(help_text="Parent region URL")
    children = serializers.SerializerMethodField(help_text="List of child region URLs")

    @extend_schema_field(serializers.URLField(allow_null=True))
    def get_parent(self, obj):
        request = self.context.get('request')
        field = obj['parent']
        return None if field is None else reverse('api-region-detail', kwargs={'pk': field}, request=request)

    @extend_schema_field(serializers.ListField(child=serializers.URLField()))
    def get_children(self, obj):
        request = self.context.get('request')
        return [
            reverse('v1:api-region-detail', kwargs={'pk': rid}, request=request)
            for rid in obj.get('children', [])
        ]


class GeoNamesSerializer(serializers.Serializer):
    geonames_id = serializers.IntegerField(source="geoNamesId", help_text="GeoNames numeric ID (see geonames.org)")
    uri = serializers.URLField(help_text="Unique URI within the 1145 namespace")
    name = serializers.SerializerMethodField(help_text="Name")
    geonames_url = serializers.URLField(source="geoNamesURL", help_text="Original GeoNames URL")
    country_code = serializers.CharField(source="countryCode", help_text="ISO country code")
    admin1_code = serializers.SerializerMethodField(help_text="State or Province. This is the GeoNames admin1_code which is a mix of ISO and FIPS codes. It is not always available")
    region_api_url = serializers.SerializerMethodField(help_text="Link to Region in Syracuse API")

    @extend_schema_field(serializers.CharField())
    def get_name(self, obj):
        name = obj.name
        return name[0] if isinstance(name, list) and name else ''

    @extend_schema_field(serializers.CharField(allow_null=True))
    def get_admin1_code(self, obj):
        return None if obj.admin1Code == '00' else obj.admin1Code

    @extend_schema_field(serializers.URLField())
    def get_region_api_url(self, obj):
        admin1 = self.get_admin1_code(obj)
        region_code = obj.countryCode
        if admin1:
            region_code = f"{region_code}-{admin1}"
        request = self.context.get('request')
        if region_code is None or region_code.strip() == '':
            return None
        return reverse('api-region-detail', kwargs={'pk': region_code}, request=request)


class ShortIndustryClusterSerializer(serializers.Serializer):
    uri = serializers.URLField(help_text="Industry cluster URI")
    representative_docs = serializers.ListField(
        child=serializers.CharField(),
        source="representativeDoc",
        help_text="List of representative document URIs or IDs"
    )
    industry_api_url = serializers.SerializerMethodField(help_text="Link to Industry Cluster in Syracuse API")

    @extend_schema_field(serializers.URLField())
    def get_industry_api_url(self, obj):
        request = self.context.get('request')
        return reverse('api-industrycluster-detail', kwargs={'pk': obj.pk}, request=request)


class ActivityActorSerializer(serializers.Serializer):
    name = serializers.CharField(source="best_name", help_text="Best available name")
    uri = serializers.URLField(help_text="Unique URI")
    based_in = GeoNamesSerializer(many=True, source="basedInHighGeoNamesLocation", help_text="Actor locations")
    industries = ShortIndustryClusterSerializer(
        many=True,
        source="industryClusterPrimary",
        required=False,
        help_text="Primary industries for the actor"
    )

class ActorRolesSerializer(serializers.Serializer):
    awarded	= ActivityActorSerializer(many=True, required=False)
    buyer = ActivityActorSerializer(many=True, required=False)
    investor = ActivityActorSerializer(many=True, required=False)
    location = ActivityActorSerializer(many=True, required=False)
    location_added_by = ActivityActorSerializer(many=True, required=False)
    location_removed_by	= ActivityActorSerializer(many=True, required=False, help_text="Organization that is removing a location (e.g. shutting down a factory). Used in **LocationActivity**")
    organization = ActivityActorSerializer(many=True, required=False, help_text="Generic term for an involved organization. Used across many activity types.")
    participant	= ActivityActorSerializer(many=True, required=False, help_text="An organization involved in a story but not who the story is about. For example a legal firm in an M&A transaction. Used in **CorporateFinanceActivity**")
    partnership	= ActivityActorSerializer(many=True, required=False, help_text="When two or more organizations partner together. Used in **PartnershipActivity**")
    person = ActivityActorSerializer(many=True, required=False, help_text="A human being. Used in **RoleActivity**")
    product	= ActivityActorSerializer(many=True, required=False, help_text="A product or service. Used in **ProductActivity**")
    protagonist	= ActivityActorSerializer(many=True, required=False, help_text="Organization that is involved in a merger where there is no obvious buyer or seller. Used in **CorporateFinanceActivity**")
    provided_by	= ActivityActorSerializer(many=True, required=False, help_text="Provider of goods and services. Used in **PartnershipActivity** where the partnership looks more like someone buying something from a supplier")
    role = ActivityActorSerializer(many=True, required=False, help_text="A senior role (e.g. Director, VP, CEO). Used in **RoleActivity**")
    target = ActivityActorSerializer(many=True, required=False, help_text="The entity that is being bought or sold or invested in. Used in **CorporateFinanceActivity**")
    vendor = ActivityActorSerializer(many=True, required=False, help_text="Seller of all or part of another organization. Used in **CorporateFinanceActivity**")


class ActivityOrIndustrySectorUpdateSerializer(serializers.Serializer):

    def to_representation(self, instance):
        if instance['activity_class'] == 'IndustrySectorUpdate':
            serializer = IndustrySectorUpdateSerializer(instance, context=self.context)
        else:
            serializer = ActivitySerializer(instance, context=self.context)
        return serializer.data
   
class ActivityIndustrySectorUpdateBaseSerializer(serializers.Serializer):
    activity_class = serializers.CharField(help_text=activity_docstring_markdown)
    headline = serializers.CharField(help_text="Source document headline")
    date_published = serializers.DateTimeField(help_text="Date of publication")
    source_organization = serializers.CharField(help_text="Organization that produced or published the source document (e.g. news provider, analyst firm)")
    document_extract = serializers.CharField(help_text="Extract from the source document")
    document_url = serializers.URLField(help_text="Source document URL")
    archive_org_page_url = serializers.URLField(help_text="Link to the source document on archive.org (will only work if archive.org has crawled this page)")
    archive_org_list_url = serializers.URLField(help_text="archive.org listing page around the time of this document (will only work if archive.org has crawled this page)")
    uri = serializers.SerializerMethodField(help_text="Unique URI for this entity within the 1145 namespace (alias for activity_uri or industry_sector_update_uri)")

    @extend_schema_field(serializers.URLField())
    def get_uri(self, obj):
        return self.uri_alias(obj)
    
    def uri_alias(self, obj):
        pass
    
class IndustrySectorUpdateSerializer(ActivityIndustrySectorUpdateBaseSerializer):
    industry_sector_update_uri = serializers.URLField(help_text="Unique URI for this industry sector update within the 1145 namespace")
    highlight = serializers.CharField(help_text="highlight text")
    industry_sector = serializers.CharField(help_text="Industry Sector name")
    industry_subsector= serializers.CharField(help_text="Industry Subsector name")
    metric = serializers.CharField(help_text="Any metric reported in this industry sector update")
    analyst_organization = ActivityActorSerializer(help_text="Organization that produced this report")

    def uri_alias(self, obj):
        return obj['industry_sector_update_uri']

class ActivitySerializer(ActivityIndustrySectorUpdateBaseSerializer):
    activity_uri = serializers.URLField(help_text="Unique URI for this activity within the 1145 namespace")
    activity_locations = GeoNamesSerializer(many=True, help_text="Geographic locations")
    actors = ActorRolesSerializer(
        help_text=("Entities involveed in this activity, grouped by their role. The types of entity and types of role depend on the type of activity:"
                    "\n\n## CorporateFinanceActivity:\n\n"
                    " - **buyer**: An organization that is buying all or part of another organization (in practice has overlap with investor)\n"
                    " - **investor**: An organization that is investing in another organization (in practice has overlap with investor)\n"
                    " - **target**: The organization that is being bought or invested in\n"
                    " - **vendor**: The organization that is selling the target\n"
                    " - **protagonist**: If it's not clear who is the buyer or the seller, e.g. in a merger, all parties are referred to as protagonists\n"
                    " - **participant**: Another related organization in the transaction, e.g. a law firm or investment bank\n"
                    "\n\n## PartnershipActivity:\n\n"
                    " - **awarded**: Where a partnership looks like buying goods or services (e.g. we partnered with such and such a CRM provider for our CRM needs), awarded means the organization that is receiving the goods or services\n" 
                    " - **partnership**: Other organization(s) that the partnership is with\n"
                    " - **provided_by**: Where a partnership looks like buying goods or services (e.g. we partnered with such and such a CRM provider for our CRM needs), awarded means the organization that is providing the goods or services\n" 
                    "\n\n## RoleActivity:\n\n" 
                    " - **role**: The role that relates to this activity\n"
                    " - **person**: The person who has either starting or stopping in this role\n"
                    " - **organization**: The organization where this activity is taking place\n"
                    "\n\n## LocationActivity:\n\n"
                    " - **location_added_by**: This organization has opened up in the related location\n"
                    " - **location_removed_by**: This organization has shut down the related location (e.g. shut down a factory)\n"
                    " - **location**: A geographical location (e.g. country, city, region)\n"
                    "\n\n## ProductActivity:\n\n"
                    " - **product**: Name of the product that was launched\n"
                    " - **organization**: Organization which launched this product\n"
                    "\n\n## AnalystRatingActivity, EquityActionsActivity, FinancialReportingActivity, FinancialsActivity, IncidentActivity, MarketingActivity, OperationsActivity, RecognitionActivity, RegulatoryActivity:\n\n"
                    " - **organization**: An organization that this activity relates to\n"
        )
    )

    def uri_alias(self, obj):
        return obj['activity_uri']