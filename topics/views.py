from django.shortcuts import render
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from topics.models import Organization, ActivityMixin, Resource
from topics.cache_helpers import is_cache_ready
from topics.model_queries import get_children_for_api, single_org_children
from .serializers import (OrganizationGraphSerializer, OrganizationSerializer,
    NameSearchSerializer, GeoSerializer, TimelineSerializer,
    IndustrySerializer,OrganizationTimelineSerializer,
    ResourceSerializer)
from rest_framework import status
from datetime import date, datetime
from .model_queries import get_relevant_orgs_for_country_region_industry
from urllib.parse import urlparse
from syracuse.settings import MOTD
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
import json
from integration.models import DataImport
from topics.faq import FAQ
from itertools import islice

import logging
logger = logging.getLogger(__name__)


class About(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = "p/about.html"

    def get(self,request):
        return Response({"faqs": FAQ}, status=status.HTTP_200_OK)

class Index(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'index.html'

    def get(self,request):
        params = request.query_params
        org_name = params.get("name")
        selected_geo_name = params.get("country_or_region")
        industry_name = params.get("industry")

        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        if selected_geo is None:
            selected_geo_name = None
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        if industry_name is None:
            industry_name = None

        if org_name:
            orgs = Organization.find_by_name(org_name)
            num_hits = len(orgs)
            if len(orgs) > 20:
                orgs = islice(orgs,20)
            org_list = OrganizationSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            geo_search = GeoSerializer()
            search_type = 'org_name'
            search_term = org_name
        elif selected_geo or industry:
            orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry)
            num_hits = len(orgs)
            if len(orgs) > 20:
                orgs = islice(orgs,20)
            org_list = OrganizationSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":""})
            geo_search = GeoSerializer()
            search_type = 'combined_search'
            search_term = industry_geo_search_str(industry_name, selected_geo_name)
        else:
            orgs = Organization.randomized_active_nodes(10)
            org_list = OrganizationSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            geo_search = GeoSerializer()
            search_type = 'random'
            search_term = None
            num_hits = 0
        industry_serializer = IndustrySerializer()
        last_updated = DataImport.latest_import_ts()
        alpha_flag = request.GET.get("alpha_flag")
        resp = Response({"organizations":org_list.data,
                        "search_serializer": org_search,
                        "search_term": search_term,
                        "num_hits": num_hits,
                        "industry_search": industry_serializer,
                        "geo_search": geo_search,
                        "search_type": search_type,
                        "motd": MOTD,
                        "alpha_flag": alpha_flag,
                        "last_updated": last_updated,
                        "search_industry_name": industry_name,
                        "search_geo_code": selected_geo,
                        "cache_ready": is_cache_ready(),
                        }, status=status.HTTP_200_OK)
        return resp


class ShowResource(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'show_resource.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}"
        doc_id = kwargs.get("doc_id")
        if doc_id is not None:
            uri = f"{uri}/{doc_id}"
        uri = f"{uri}/{kwargs['name']}"
        r = Resource.nodes.get(uri=uri)
        if r is None:
            raise ValueError(f"Couldn't find Resource with uri {uri}")
        if isinstance(r, Organization):
            self.template_name = 'organization_linkages.html'
            org_serializer = OrganizationGraphSerializer(r)
            org_data = {**kwargs, **{"uri":r.uri,"source_node_name":r.best_name}}
            resp = Response({"data_serializer": org_serializer.data,
                                "org_data":org_data,
                                "cache_ready": is_cache_ready(),
                                }, status=status.HTTP_200_OK)
            return resp
        else:
            resource_serializer = ResourceSerializer(r)
            resp = Response({"data_serializer":resource_serializer.data,
                            "cache_ready": is_cache_ready(),
                            }, status=status.HTTP_200_OK)
            return resp



class ParentChildWithSearch(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'parent_child_with_search.html'

    def get(self, request):
        params = request.query_params
        org_name = params.get("name")
        org_search = NameSearchSerializer({"name":org_name})
        if org_name is None or org_name == '':
            return Response({"search_serializer": org_search},status=status.HTTP_200_OK)
        orgs = Organization.find_by_name(org_name)
        if len(orgs) == 0:
            logger.debug(f"Couldn't find org with {org_name}")
        orgs_and_children = get_children_for_api(orgs)
        return Response({"orgs_and_children": orgs_and_children,
                        "orgs_with_completed_children": sum(
                            [x["completed_children_count"] > 0 for x in orgs_and_children ]),
                        "search_serializer": org_search,
                        "cache_ready": is_cache_ready(),
                        }, status=status.HTTP_200_OK)

class OrganizationChildrenList(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_children_list.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Organization.nodes.get(uri=uri)
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        children = single_org_children(o)
        completed_children_count = sum([x["activity_status_as_string"]=="completed" for x in children])
        orgs_and_children = get_children_for_api(o.sameAsNameOnly)
        resp = Response({"org_children": children,
                            "completed_children_count": completed_children_count,
                            "org_data":org_data,
                            "cache_ready": is_cache_ready(),
                            "orgs_and_children":orgs_and_children,
                            }, status=status.HTTP_200_OK)
        return resp

class TopicsTimeline(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'topics_timeline.html'

    def get(self, request):
        params = request.query_params
        industry = params["industry_name"]
        orgs = Organization.find_by_industry(industry)
        timeline_serializer = TimelineSerializer(orgs)
        return Response({"industry_name":industry,
                            "timeline_serializer": timeline_serializer.data,
                            "cache_ready": is_cache_ready(),
                            }, status=status.HTTP_200_OK)

class OrganizationTimeline(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_timeline.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Organization.nodes.get(uri=uri)
        org_serializer = OrganizationTimelineSerializer(o)
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        resp = Response({"timeline_serializer": org_serializer.data,
                            "org_data":org_data,
                            "cache_ready": is_cache_ready(),
                            }, status=status.HTTP_200_OK)
        return resp


class RandomOrganization(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_linkages.html'

    def get(self, request):
        o = Organization.get_random()
        vals = elements_from_uri(o.uri)
        org_serializer = OrganizationGraphSerializer(o)
        resp = Response({"data_serializer": org_serializer.data,
                            "org_data":vals,
                            "cache_ready": is_cache_ready(),}, status=status.HTTP_200_OK)
        return resp


class OrganizationByUri(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_linkages.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Organization.nodes.get(uri=uri)
        org_serializer = OrganizationGraphSerializer(o)
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        resp = Response({"data_serializer": org_serializer.data,
                            "org_data":org_data,
                            "cache_ready": is_cache_ready(),
                            }, status=status.HTTP_200_OK)
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


def industry_geo_search_str(industry, geo,with_emphasis=True):
    industry_str = "all industries" if industry is None or industry.strip() == '' else industry
    geo_str = "all locations" if geo is None or geo.strip() == '' else geo
    if geo_str.split()[0].lower() == 'united':
        in_str = "in the"
    else:
        in_str = "in"
    return f"<b>{industry_str.title()}</b> {in_str} <b>{geo_str.title()}</b>"
