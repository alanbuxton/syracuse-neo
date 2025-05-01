from rest_framework.response import Response
from topics.models import IndustryCluster
import api.serializers as serializers
import logging 
from rest_framework import status
from rest_framework.viewsets import GenericViewSet
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from topics.industry_geo import GEO_PARENT_CHILDREN

logger = logging.getLogger(__name__)

class NeomodelViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    node_class = None
    serializer_class = None

    def get_queryset(self):
        return self.node_class.nodes.all()

    def get_object(self):
        return self.node_class.nodes.get(internalId=self.kwargs["pk"])

    def get_serializer_context(self):
        node_class_name = self.node_class.__name__.lower()
        return {
            "request": self.request,
            "view_name": f"api-{node_class_name}-detail",
            "attribs_to_ignore": self.attribs_to_ignore,
        }

    def retrieve(self, request, pk=None):
        instance = self.get_object()
        serializer = serializers.HyperlinkedNeomodelSerializer(
            instance,
            context=self.get_serializer_context()
        )
        return Response(serializer.data)
    
    def list(self, request):
        nodes = self.get_queryset()
        page = self.paginate_queryset(nodes)
        if page is not None:
            serializer = serializers.HyperlinkedNeomodelSerializer(
                page,
                context=self.get_serializer_context(),
                many=True,
            )
            return self.get_paginated_response(serializer.data)
        
        serializer = serializers.HyperlinkedNeomodelSerializer(
                nodes,
                context=self.get_serializer_context(),
                many=True,
            )
        return Response(serializer.data)
        
class IndustryClusterViewSet(NeomodelViewSet):
    node_class = IndustryCluster  

    attribs_to_ignore = ["foundName","name","internalDocId",
                         "internalMergedSameAsToHighUri","documentSource","sameAsHigh",
                         "orgsPrimary","orgsSecondary","peoplePrimary","peopleSecondary"]


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
