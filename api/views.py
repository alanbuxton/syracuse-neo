from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from auth_extensions.anon_user_utils import IsAuthenticatedNotAnon
from topics.models import IndustryCluster, GeoNamesLocation, Resource
import api.serializers as serializers
import logging 
from topics.util import geo_to_country_admin1
from rest_framework import status
from topics.activity_helpers import get_activities_by_org_uris_and_date_range, get_activities_by_industry_country_admin1_and_date_range
from rest_framework.viewsets import GenericViewSet, ReadOnlyModelViewSet
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication
from syracuse.authentication import FlexibleTokenAuthentication
from topics.industry_geo import geo_parent_children, geo_codes_for_region
from topics.organization_search_helpers import search_organizations_by_name 
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from drf_spectacular.types import OpenApiTypes
import re
from api.models import APIRequestLog
from django.utils.dateparse import parse_date
from django.core.paginator import Paginator
from rest_framework.views import APIView
from syracuse.date_util import min_and_max_date
from django.http import Http404
from django.core.exceptions import ObjectDoesNotExist
from neomodel import DoesNotExist
from api.docstrings import activity_docstring_raw
from syracuse.cache_util import get_versionable_cache, set_versionable_cache

logger = logging.getLogger(__name__)

class NeomodelViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]

    node_class = None
    serializer_class = serializers.HyperlinkedNeomodelSerializer
    lookup_field = "uri"
    lookup_url_kwarg = "uri"

    def get_queryset(self):
        if self.node_class is None:
            raise Http404("Not found")
        return self.node_class.nodes.all()

    def get_object(self):
        pk = self.kwargs.get('pk') or self.kwargs.get('uri')
        if pk is None:
            raise Http404("Not found")
        uri = pk.rstrip("/")
        try:
            return Resource.nodes.get(uri=uri)
        except (Http404, DoesNotExist, ObjectDoesNotExist):
            raise Http404(f"No object found for {uri}")

    def get_serializer_context(self):
        node_class_name = self.node_class.__name__.lower()
        return {
            "request": self.request,
            "view_name": f"api-{node_class_name}-detail",
        }

    def retrieve(self, request, **kwargs):
        instance = self.get_object()      
        serializer = self.serializer_class(
            instance,
            context=self.get_serializer_context()
        )
        return Response(serializer.data, status=status.HTTP_200_OK)
    
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
        return Response(serializer.data, status=status.HTTP_200_OK)
    
        
class IndustryClusterViewSet(NeomodelViewSet):
    node_class = IndustryCluster  
    serializer_class = serializers.IndustryClusterSerializer
    
    lookup_field = "pk"
    lookup_url_kwarg = None
    
    def get_object(self):
        try:
            return self.node_class.nodes.get(topicId=self.kwargs["pk"])
        except (Http404, DoesNotExist, ObjectDoesNotExist):
            raise Http404(f"No IndustryCluster found for {self.kwargs['pk']} (must be numeric)")
        
    @extend_schema(
        parameters=[
            OpenApiParameter(
                name='id',
                description='Unique ID for this IndustryCluster - also referred to as topic_id',
                type=OpenApiTypes.INT,
                location=OpenApiParameter.PATH
            ),
        ]
    )
    def retrieve(self, request, **kwargs):
        return super().retrieve(request, **kwargs)

class RegionsViewSet(GenericViewSet):
    """
        Regions groups ISO-3166 country codes using the United Nations M49 standard (Region/Sub-Region/Intermediate Region).
        The US is further broken down United States Census Bureau regions (e.g. East/West etc) and then into individual states.
        Certain other countries are also broken down into their states/provinces: AE, CA, CN, IN. Individual GeoNames locations are
        linked to a country and/or state/province.
    """
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]
    serializer_class = serializers.RegionsDictSerializer

    lookup_field = "pk"
    lookup_url_kwarg = None

    def get_queryset(self):
        return geo_parent_children().values()

    def get_object(self):
        pk = self.kwargs["pk"]
        obj = geo_parent_children().get(pk)
        if obj is None:
            raise Http404(f"No Region found for {pk}")
        return obj

    def list(self, request):
        serializer = self.get_serializer(self.get_queryset(), many=True, context={'request': request})
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    @extend_schema(
        parameters=[
            OpenApiParameter(
                name='id',
                description='Unique ID for this Region (e.g. Americas, Northern America, US, New England, US-MA). Case matters.',
                type=OpenApiTypes.STR,
                location=OpenApiParameter.PATH
            ),
        ]
    )      
    def retrieve(self, request, pk=None):
        item = self.get_object()
        serializer = self.get_serializer(item, context={'request': request})
        return Response(serializer.data, status=status.HTTP_200_OK)

class GeoNamesViewSet(NeomodelViewSet):
    """
       GeoNames Locations are from https://www.geonames.org. They are grouped within the Regions hierarchy and are linked by
       ISO-3166 country code. For AE, CA, CN, IN, US country codes, GeoNames are also grouped by state/province, if available.
       GeoNames uses a mix of FIPS and ISO codes for state/province, and those are stored in the admin1_code field. An admin1_code
       of 00 means "not applicable".
    """
        
    node_class = GeoNamesLocation
    serializer_class = serializers.GeoNamesSerializer

    lookup_field = "pk"
    lookup_url_kwarg = None

    def get_object(self):
        pk = self.kwargs["pk"]
        try:
            pk = int(pk)
        except ValueError:
            raise Http404(f"No GeoName found for {pk} (must be a numeric GeoNames ID, e.g. 5128581 for New York City)")
        
        try:
            return self.node_class.nodes.get(geoNamesId=pk)
        except (Http404, ObjectDoesNotExist, DoesNotExist):
            raise Http404(f"No GeoName found for {pk} (must be a numeric GeoNames ID), e.g. 5128581 for New York City)")
        
    @extend_schema(    
        parameters=[
            OpenApiParameter("id", OpenApiTypes.INT, OpenApiParameter.PATH)
        ],
        responses={
            200:OpenApiResponse(
            description='GeoNames entity'
        )
        }
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    

@extend_schema(
        summary="Activities",
        description=activity_docstring_raw
)
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
        locations = self.request.query_params.getlist('location_id',[])
        industry_search_str = self.request.query_params.getlist('industry_name',[])
        industry_ids = self.request.query_params.getlist('industry_id',[None])

        if org_uri is None and org_name is None and len(locations) == 0 and len(industry_search_str) == 0 and industry_ids == [None]:
            return None 
        
        cache_key = f"{min_date}_{max_date}_{org_uri}_{org_name}_{types_to_keep}_{locations}_{industry_search_str}_{industry_ids}"

        if self.request.query_params.get("no_cache"):
            logger.info("Bypassing cache")
        else:
            res = get_versionable_cache(cache_key)
            if res:
                return res

        if org_uri is not None:
            logger.debug(f"Uri: {org_uri}")
            activities = get_activities_by_org_uris_and_date_range([org_uri],min_date,max_date,combine_same_as_name_only=True,limit=None)
        elif org_name is not None:
            orgs_and_counts = search_organizations_by_name(org_name, combine_same_as_name_only=False, top_1_strict=True, request=self.request)
            uris = [x.uri for x,_ in orgs_and_counts]
            logger.debug(f"Uris: {uris}")
            activities = get_activities_by_org_uris_and_date_range(uris,min_date,max_date,combine_same_as_name_only=True,limit=None)
        else:
            if len(industry_search_str) > 0:
                industry_ids = set()
                for search_str in industry_search_str:
                    inds = [x.topicId for x in IndustryCluster.by_name(search_str,limit=3,request=self.request)]
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
        set_versionable_cache(cache_key, acts, timeout=3600)
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
                description='Show activities this many days old. Optional, but if provided must be one of: 7, 30, or 90 (default). Any other number will be treated as 90.',
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
                name='region_id',
                description=('List of region identifiers (e.g., "Southern Asia", "BT", "US-CA"). Accepts multiple values.'
                    ' They must each match an id from the [regions](#/regions/regions_list) endpoint.'            
                ),
                required=False,
                type=OpenApiTypes.STR,
                many=True,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='industry_name',
                description='List of industry names to filter by. Accepts multiple values.',         
                required=False,
                type=OpenApiTypes.STR,
                many=True,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='industry_id',
                description=('List of industry IDs (topic IDs) to filter by. Accepts multiple values.'
                             ' They must each match a topic_id from the [industry_clusters](#/industry_clusters/industry_clusters_list) endpoint.'          
                ),
                required=False,
                type=OpenApiTypes.INT,
                many=True,
                location=OpenApiParameter.QUERY
            ),
        ],
        responses={
            200:OpenApiResponse(
            response=serializers.ActivitySerializer,
            description='Paginated list of activities'
        )
        }
    )
    def list(self, request):
        """
            List recent relevant activities.

            Must provide at least one of `org_name`, `org_uri`, `region_id`, `industry_name` or `industry_id`

            By default shows activities over the last 90 days, but you can also request 7 or 30 days.
        """
        activities = self.get_queryset()
        if activities is None:
            msg = "Must include at least one of org_uri, org_name, region_id, industry_name, industry_id (location_id, industry_name and industry_id can be specified multiple times)"
            resp = Response({"message":msg},status=status.HTTP_400_BAD_REQUEST)
            return resp
        page = self.paginate_queryset(activities)
        if page is not None:
            serializer = serializers.ActivitySerializer(
                page,
                context=self.get_serializer_context(),
                many=True,
            )
            resp = self.get_paginated_response(serializer.data)
            return resp
        
        serializer = serializers.ActivitySerializer(
                activities,
                context=self.get_serializer_context(),
                many=True,
            )
        resp = Response(serializer.data, status=status.HTTP_200_OK)
        return resp
    
    @extend_schema(
        parameters=[
            OpenApiParameter("uri", OpenApiTypes.URI, OpenApiParameter.PATH)
        ],   
        responses={
               200: OpenApiResponse(
                response=serializers.ActivitySerializer,
                description='Activity details'
            )
        }
    )
    def retrieve(self, request, *args, **kwargs):
        """
            Returns one Activity. Use URI as the primary key (e.g. https://syracuse.1145.am/api/v1/activities/https://1145.am/db/12345/activity_uri)
        """
        return super().retrieve(request, *args, **kwargs)
    

class APIUsageViewSet(ReadOnlyModelViewSet):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'api_usage.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]

    def get_queryset(self):
        return APIRequestLog.objects.filter(user=self.request.user)

    def list(self, request, *args, **kwargs):
        logs = self.get_queryset()

        # Date filtering
        start_date = request.GET.get('start')
        end_date = request.GET.get('end')
        if start_date:
            logs = logs.filter(timestamp__gte=parse_date(start_date))
        if end_date:
            logs = logs.filter(timestamp__lte=parse_date(end_date))

        # Sorting
        order_by = request.GET.get('sort', '-timestamp')
        if order_by.lstrip('-') in ['timestamp', 'path', 'method', 'status_code', 'duration']:
            logs = logs.order_by(order_by)

        # Pagination
        paginator = Paginator(logs, 25)  # 25 per page
        page = request.GET.get('page')
        page_obj = paginator.get_page(page)

        return Response({
            'token': request.user.auth_token.key if hasattr(request.user, 'auth_token') else None,
            'page_obj': page_obj,
            'order_by': order_by,
            'start_date': start_date or '',
            'end_date': end_date or '',
        }, status=status.HTTP_200_OK)
    

class APITokenView(APIView):
    authentication_classes = [SessionAuthentication, FlexibleTokenAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        token_key = request.user.auth_token.key
        return Response({'token': token_key}, status=status.HTTP_200_OK)

 
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
