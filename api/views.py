from rest_framework.response import Response
from topics.models import IndustryCluster
import api.serializers as serializers
import logging 
from topics.util import min_and_max_date
from rest_framework import status
from topics.activity_helpers import get_activities_by_org_uris_and_date_range
from rest_framework.viewsets import GenericViewSet
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from topics.industry_geo import GEO_PARENT_CHILDREN, geo_codes_for_region, org_uris_by_industry_and_or_geo
from topics.organization_search_helpers import search_organizations_by_name 

logger = logging.getLogger(__name__)

class NeomodelViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

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

class GeosViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = serializers.GeoDictSerializer

    def get_queryset(self):
        return GEO_PARENT_CHILDREN.values()

    def get_object(self):
        return GEO_PARENT_CHILDREN.get(self.kwargs["pk"])

    def list(self, request):
        serializer = self.get_serializer(self.get_queryset(), many=True, context={'request': request})
        return Response(serializer.data)
    
    def retrieve(self, request, pk=None):
        item = self.get_object()
        if item is None:
            return Response({'detail': 'Not found.'}, status=status.HTTP_404_NOT_FOUND)
        
        serializer = self.serializer_class(item, context={'request': request})
        return Response(serializer.data)


class ActivitiesViewSet(NeomodelViewSet):

    def get_queryset(self):
        min_date, max_date = min_and_max_date(self.request.GET)
        org_uri = self.request.query_params.get("org_uri",None)
        org_name = self.request.query_params.get("org_name",None)
        if org_uri is not None:
            logger.info(f"Uri: {org_uri}")
            activities = get_activities_by_org_uris_and_date_range([org_uri],min_date,max_date,combine_same_as_name_only=True,limit=None)
        elif org_name is not None:
            orgs_and_counts = search_organizations_by_name(org_name, combine_same_as_name_only=False, limit=20)
            uris = [x.uri for x,_ in orgs_and_counts]
            logger.info(f"Uris: {uris}")
            activities = get_activities_by_org_uris_and_date_range(uris,min_date,max_date,combine_same_as_name_only=True,limit=None)
        else:
            locations = self.request.query_params.getlist('location_id',[])
            industry_search_str = self.request.query_params.getlist('industry_name',[])
            industry_ids = self.request.query_params.getlist('industry_id',[None])
            if len(industry_search_str) > 0:
                industry_ids = set()
                for search_str in industry_search_str:
                    inds = [x.topicId for x in IndustryCluster.by_representative_doc_words(search_str)]
                    industry_ids.update(inds)
            geo_codes = set()
            for loc in locations:
                geo_codes.update(geo_codes_for_region(loc))
            if len(geo_codes) == 0:
                geo_codes = [None]
            logger.info(f"Industry ids: {industry_ids}, geo: {geo_codes}, {min_date} - {max_date}")
            activities = []
            for industry_id_str in industry_ids:
                if industry_id_str is not None:
                    industry_id = int(industry_id_str)
                else:
                    industry_id = industry_id_str
                for geo_code in geo_codes:
                    uri_list = org_uris_by_industry_and_or_geo(industry_id, geo_code, return_orgs_only=True)
                    acts = get_activities_by_org_uris_and_date_range(uri_list,min_date,max_date,combine_same_as_name_only=True,limit=None)
                    activities.extend(acts)
        acts = sorted(activities, key = lambda x: x['date_published'], reverse=True)
        return acts
    
    def get_serializer_context(self):
         return {
            "request": self.request,
            "view_name": f"api-activities-detail",
        }

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
