from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from auth_extensions.anon_user_utils import IsAuthenticatedNotAnon
from topics.models import IndustryCluster, GeoNamesLocation
import api.serializers as serializers
import logging 
from topics.util import min_and_max_date, geo_to_country_admin1
from rest_framework import status
from topics.activity_helpers import get_activities_by_org_uris_and_date_range, get_activities_by_industry_country_admin1_and_date_range
from rest_framework.viewsets import GenericViewSet, ReadOnlyModelViewSet
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication
from syracuse.authentication import FlexibleTokenAuthentication
from topics.industry_geo import geo_parent_children, geo_codes_for_region
from topics.organization_search_helpers import search_organizations_by_name 
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
import re
from rest_framework.authtoken.models import Token

logger = logging.getLogger(__name__)

class NeomodelViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]

    node_class = None
    serializer_class = serializers.HyperlinkedNeomodelSerializer

    def get_queryset(self):
        return self.node_class.nodes.all()

    def get_object(self):
        return self.node_class.nodes.get(uri=self.kwargs["pk"])

    def get_serializer_context(self):
        node_class_name = self.node_class.__name__.lower()
        return {
            "request": self.request,
            "view_name": f"api-{node_class_name}-detail",
        }

    def retrieve(self, request, pk=None):
        instance = self.get_object()
        serializer = self.serializer_class(
            instance,
            context=self.get_serializer_context()
        )
        return Response(serializer.data)
    
    def list(self, request):
        nodes = self.get_queryset()
        page = self.paginate_queryset(nodes)
        if page is not None:
            serializer = self.serializer_class(
                page,
                context=self.get_serializer_context(),
                many=True,
            )
            return self.get_paginated_response(serializer.data)
        
        serializer = self.serializer_class(
                nodes,
                context=self.get_serializer_context(),
                many=True,
            )
        return Response(serializer.data)
    
        
class IndustryClusterViewSet(NeomodelViewSet):
    node_class = IndustryCluster  
    serializer_class = serializers.IndustryClusterSerializer
    
    def get_object(self):
        return self.node_class.nodes.get(topicId=self.kwargs["pk"])

class RegionsViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]
    serializer_class = serializers.RegionsDictSerializer

    def get_queryset(self):
        return geo_parent_children().values()

    def get_object(self):
        return geo_parent_children().get(self.kwargs["pk"])

    def list(self, request):
        serializer = self.get_serializer(self.get_queryset(), many=True, context={'request': request})
        return Response(serializer.data)
    
    def retrieve(self, request, pk=None):
        item = self.get_object()
        if item is None:
            return Response({'detail': 'Not found.'}, status=status.HTTP_404_NOT_FOUND)
        
        serializer = self.serializer_class(item, context={'request': request})
        return Response(serializer.data)

class GeoNamesViewSet(NeomodelViewSet):
    node_class = GeoNamesLocation
    serializer_class = serializers.GeoNamesSerializer

    def get_object(self):
        return self.node_class.nodes.get(geoNamesId=int(self.kwargs["pk"]))

class ActivitiesViewSet(NeomodelViewSet):

    def get_queryset(self):
        days_ago = self.request.query_params.get("days_ago","90")
        days_ago = int(days_ago)
        if days_ago not in [7,30]:
            days_ago = 90
        min_date, max_date = min_and_max_date(self.request.GET, days_diff=days_ago)
        org_uri = self.request.query_params.get("org_uri",None)
        org_name = self.request.query_params.get("org_name",None)
        types_to_keep = self.request.query_params.getlist("type",[])
        if org_uri is not None:
            logger.debug(f"Uri: {org_uri}")
            activities = get_activities_by_org_uris_and_date_range([org_uri],min_date,max_date,combine_same_as_name_only=True,limit=None)
        elif org_name is not None:
            orgs_and_counts = search_organizations_by_name(org_name, combine_same_as_name_only=False, top_1_strict=True)
            uris = [x.uri for x,_ in orgs_and_counts]
            logger.debug(f"Uris: {uris}")
            activities = get_activities_by_org_uris_and_date_range(uris,min_date,max_date,combine_same_as_name_only=True,limit=None)
        else:
            locations = self.request.query_params.getlist('location_id',[])
            industry_search_str = self.request.query_params.getlist('industry_name',[])
            industry_ids = self.request.query_params.getlist('industry_id',[None])
            if len(industry_search_str) > 0:
                industry_ids = set()
                for search_str in industry_search_str:
                    inds = [x.topicId for x in IndustryCluster.by_representative_doc_words(search_str,limit=3)]
                    industry_ids.update(inds)
            geo_codes = set()
            for loc in locations:
                geo_codes.update(geo_codes_for_region(loc))
            if len(geo_codes) == 0:
                geo_codes = [None]
            logger.debug(f"Industry ids: {industry_ids}, geo: {geo_codes}, {min_date} - {max_date}")
            activities = []
            for industry_id_str in industry_ids:
                if industry_id_str is not None:
                    industry_id = int(industry_id_str)
                else:
                    industry_id = industry_id_str
                for geo_code in geo_codes:
                    country_code, admin1 = geo_to_country_admin1(geo_code)
                    acts = get_activities_by_industry_country_admin1_and_date_range(industry_id, country_code, admin1, min_date, max_date)
                    activities.extend(acts)
        if len(types_to_keep) > 0:
            activities = filter_activity_types(activities, types_to_keep)
        acts = sorted(activities, key = lambda x: x['date_published'], reverse=True)
        return acts
    
    def get_serializer_context(self):
         return {
            "request": self.request,
            "view_name": f"api-activity-detail",
        }
    
    @extend_schema(
        parameters=[
            OpenApiParameter(
                name='days_ago',
                description='Show activities this many days old. Optional, but if provided must be one of: 7 (default), 30, 90.',
                required=False,
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='org_uri',
                description='Filter by organization URI (optional).',
                required=False,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='org_name',
                description='Filter by organization name (optional, partial match).',
                required=False,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='location_id',
                description=('List of location identifiers (e.g., "Southern Asia", "BT", "US-CA"). Accepts multiple values.'
                    ' They must each match an id from the [regions](#/regions/regions_list) endpoint.'            
                ),
                required=False,
                type=OpenApiTypes.STR,
                many=True,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='industry_name',
                description=('List of industry names to filter by. Accepts multiple values.'
                    ' They must each match a topic_id from the [industry_clusters](#/industry_clusters/industry_clusters_list) endpoint.'             
                ),
                required=False,
                type=OpenApiTypes.STR,
                many=True,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='industry_id',
                description='List of industry IDs (topic IDs) to filter by. Accepts multiple values.',
                required=False,
                type=OpenApiTypes.INT,
                many=True,
                location=OpenApiParameter.QUERY
            ),
        ]
    )
    def list(self, request):
        activities = self.get_queryset()
        page = self.paginate_queryset(activities)
        if page is not None:
            serializer = serializers.ActivitySerializer(
                page,
                context=self.get_serializer_context(),
                many=True,
            )
            return self.get_paginated_response(serializer.data)
        
        serializer = serializers.ActivitySerializer(
                activities,
                context=self.get_serializer_context(),
                many=True,
            )
        return Response(serializer.data)


class APIUsageViewSet(ReadOnlyModelViewSet):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'api_usage.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]

    def get_queryset(self):
        return Token.objects.get(user=self.request.user)
    
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        return Response({'tokens': queryset},status=status.HTTP_200_OK)

 
def filter_activity_types(activities, activity_types_to_keep):
    if not isinstance(activity_types_to_keep, list) and len(activity_types_to_keep) == 0:
        return activities
    activities_to_keep = []
    for act in activities:
        if any( [ re.match(
                        x.lower(),act["activity_class"].lower()) 
                        for x in activity_types_to_keep ] ):
                activities_to_keep.append(act)
    return activities_to_keep
