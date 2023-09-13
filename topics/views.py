from django.shortcuts import render
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from topics.models import Organization, WhereGeoMixin
from .serializers import (OrganizationGraphSerializer, OrganizationSerializer,
    NameSearchSerializer, DateRangeSerializer, GeoSerializer)
from rest_framework import status
from datetime import date
from .geo_utils import COUNTRY_NAMES, COUNTRY_CODES

class Index(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'index.html'

    def get(self,request):
        params = request.query_params
        org_name = params.get("name")
        country = params.get("selected_country")
        if org_name:
            orgs = Organization.nodes.filter(name__icontains=org_name)
            org_list = OrganizationSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            geo_serializer = GeoSerializer(choices=COUNTRY_NAMES)
            search_type = 'org_name'
            num_hits = len(orgs)
            search_term = org_name
        elif country:
            orgs = Organization.based_in_country(country)
            orgs_by_activity = WhereGeoMixin.orgs_by_activity_where(country)
            all_orgs = set(orgs + orgs_by_activity)
            org_list = OrganizationSerializer(all_orgs, many=True)
            org_search = NameSearchSerializer({"name":""})
            geo_serializer = GeoSerializer(choices=COUNTRY_NAMES)
            search_type = 'country'
            search_term = COUNTRY_CODES[country]
            num_hits = len(all_orgs)
        else:
            orgs = Organization.nodes.order_by('?')[:10]
            org_list = OrganizationSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            geo_serializer = GeoSerializer(choices=COUNTRY_NAMES)
            search_type = 'random'
            search_term = None
            num_hits = 0
        orgs_to_show = org_list.data
        if len(orgs_to_show) > 20:
            orgs_to_show = orgs_to_show[:20]
        resp = Response({"organizations":orgs_to_show,
                        "search_serializer": org_search,
                        "selected_country": geo_serializer,
                        "search_term": search_term,
                        "num_hits": num_hits,
                        "search_type": search_type}, status=status.HTTP_200_OK)
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
