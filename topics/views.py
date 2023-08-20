from django.shortcuts import render
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from topics.models import Organization
from .serializers import OrganizationGraphSerializer, OrganizationSerializer, SearchSerializer
from rest_framework import status

class Index(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'index.html'

    def get(self,request):
        orgs = Organization.nodes.order_by('?')[:10]
        serializer = OrganizationSerializer(orgs, many=True)
        search_serializer = SearchSerializer()
        resp = Response({"organizations":serializer.data,
                        "search_serializer": search_serializer,
                        "search_for": ''}, status=status.HTTP_200_OK)
        return resp

    def post(self, request, *args, **kwargs):
        data = request.data
        search_for = data.get('search_for')
        orgs = Organization.nodes.filter(name__icontains=search_for)
        serializer = OrganizationSerializer(orgs, many=True)
        search_serializer = SearchSerializer()
        number_of_hits = len(orgs)
        resp = Response({"organizations":serializer.data,
                        "search_serializer": search_serializer,
                        "search_for": search_for,
                        "num_hits": number_of_hits}, status=status.HTTP_200_OK)
        return resp


class RandomOrganization(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'topic_details.html'

    def get(self, request):
        o = Organization.get_random()
        return org_and_related_nodes(o)


class OrganizationByUri(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'topic_details.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Organization.nodes.get(uri=uri)
        return org_and_related_nodes(o)

def org_and_related_nodes(org):
    serializer = OrganizationGraphSerializer(org)
    resp = Response(serializer.data, status=status.HTTP_200_OK)
    return resp
