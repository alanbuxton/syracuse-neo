from django.shortcuts import render
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from topics.models import Organization, ActivityMixin
from .serializers import (OrganizationGraphSerializer, OrganizationSerializer,
    NameSearchSerializer, DateRangeSerializer, GeoSerializer, TimelineSerializer,
    IndustrySearchSerializer)
from rest_framework import status
from datetime import date
from .geo_utils import COUNTRY_NAMES, COUNTRY_CODES
from .feedback_controller import store_feedback
from urllib.parse import urlparse

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
            orgs_by_activity = ActivityMixin.orgs_by_activity_where(country)
            all_orgs = set(orgs + orgs_by_activity)
            org_list = OrganizationSerializer(all_orgs, many=True)
            org_search = NameSearchSerializer({"name":""})
            geo_serializer = GeoSerializer(choices=COUNTRY_NAMES,initial=country)
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
        industry_serializer = IndustrySearchSerializer()
        resp = Response({"organizations":orgs_to_show,
                        "search_serializer": org_search,
                        "selected_country": geo_serializer,
                        "search_term": search_term,
                        "num_hits": num_hits,
                        "industry_serializer": industry_serializer,
                        "search_type": search_type}, status=status.HTTP_200_OK)
        return resp

class TopicsTimeline(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'topics_timeline.html'

    def get(self, request):
        params = request.query_params
        industry = params["industry_name"]
        orgs = Organization.find_by_industry(industry)
        timeline_serializer = TimelineSerializer(orgs)
        return Response({"industry_name":industry,"timeline_serializer": timeline_serializer.data}, status=status.HTTP_200_OK)

class RandomOrganization(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'topic_details.html'

    def get(self, request):
        o = Organization.get_random()
        vals = elements_from_uri(o.uri)
        return org_and_related_nodes(o,vals)


class ReportIssue(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'issue_feedback.html'

    def post(self, request, *args, **kwargs):
        data = request.data
        node_or_edge = data.get('node_or_edge')
        unique_id = data.get('idval')
        reason = data.get('reason')
        source_page = request.headers['Referer']
        feedback_id, feedback_error = store_feedback(node_or_edge, unique_id, reason)
        resp = Response({"node_or_edge": node_or_edge, "unique_id": unique_id,
                        "feedback_id": feedback_id,
                        "feedback_error": feedback_error,
                        "reason": reason, "source_page": source_page}, status=status.HTTP_200_OK)
        return resp

class OrganizationByUri(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'topic_details.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Organization.nodes.get(uri=uri)
        return org_and_related_nodes(o,kwargs)

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


def org_and_related_nodes(org,org_data):
    org_serializer = OrganizationGraphSerializer(org)
    filter_serializer = DateRangeSerializer()
    resp = Response({"data_serializer": org_serializer.data, "filter_serializer": filter_serializer,
                        "org_data":org_data}, status=status.HTTP_200_OK)
    return resp

def elements_from_uri(uri):
    parsed = urlparse(uri)
    part_pieces = parsed.path.split("/")
    path = part_pieces[1]
    doc_id = part_pieces[2]
    org_name = "/".join(part_pieces[3:])
    return {
        "domain": parsed.netloc,
        "path": path,
        "doc_id": doc_id,
        "name": org_name,
    }
