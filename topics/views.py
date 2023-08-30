from django.shortcuts import render
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from topics.models import Organization
from .serializers import OrganizationGraphSerializer, OrganizationSerializer, SearchSerializer, DateRangeSerializer
from rest_framework import status
from datetime import date

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
        search_serializer = SearchSerializer({"search_for":search_for})
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
        org_serializer = OrganizationGraphSerializer(o)
        filter_serializer = DateRangeSerializer()
        resp = Response({"data_serializer": org_serializer.data, "filter_serializer": filter_serializer,
                            "org_data":kwargs}, status=status.HTTP_200_OK)
        return resp

    def post(self, request, *args, **kwargs):
        data = request.data
        from_date = data.get('from_date')
        if from_date:
            from_date = date.fromisoformat(from_date)
        to_date = data.get('to_date')
        if to_date:
            to_date = date.fromisoformat(to_date)
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Organization.nodes.get(uri=uri)
        org_serializer = OrganizationGraphSerializer(o,context={"from_date":from_date,"to_date":to_date})
        filter_serializer = DateRangeSerializer({"from_date":from_date,"to_date":to_date})
        resp = Response({"data_serializer": org_serializer.data, "filter_serializer": filter_serializer,
                            "org_data":kwargs}, status=status.HTTP_200_OK)
        return resp



def org_and_related_nodes(org,**kwargs):
    serializer = OrganizationGraphSerializer(org,**kwargs)
    resp = Response({"data":serializer.data}, status=status.HTTP_200_OK)
    return resp
