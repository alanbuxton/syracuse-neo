from django.shortcuts import render
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from topics.models import Organization, ActivityMixin, Resource
from .serializers import (OrganizationGraphSerializer, OrganizationSerializer,
    NameSearchSerializer, GeoSerializer, 
    IndustrySerializer,OrganizationTimelineSerializer,
    ResourceSerializer, FamilyTreeSerializer)
from rest_framework import status
from datetime import date, datetime
from precalculator.models import cache_last_updated_date
from urllib.parse import urlparse, urlencode
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

        request_state, combine_same_as_name_only = prepare_request_state(request)

        if org_name:
            orgs = Organization.find_by_name(org_name, combine_same_as_name_only)
            num_hits = len(orgs)
            if len(orgs) > 20:
                orgs = islice(orgs,20)
            org_list = OrganizationSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            geo_search = GeoSerializer()
            search_type = 'org_name'
            search_term = org_name
        elif selected_geo or industry:
            orgs = Organization.by_industry_and_or_geo(industry,selected_geo)
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
                        "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp


class ShowResource(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'show_resource.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}"
        request_state, combine_same_as_name_only = prepare_request_state(request)
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
                                "request_state": request_state,
                                }, status=status.HTTP_200_OK)
            return resp
        else:
            resource_serializer = ResourceSerializer(r)
            resp = Response({"data_serializer":resource_serializer.data,
                            "request_state": request_state,
                            }, status=status.HTTP_200_OK)
            return resp

class FamilyTree(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_family_tree.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        request_state, combine_same_as_name_only = prepare_request_state(request)
        request_state["hide_link"]="organization_family_tree"
        o = Organization.self_or_ultimate_target_node(uri)
        uri_parts = elements_from_uri(o.uri)
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":combine_same_as_name_only})

        return Response({"nodes_edges":nodes_edges.data,
                            "requested_uri": uri,
                            "org_data": o,
                            "uri_parts": uri_parts,
                            "request_state": request_state}, status=status.HTTP_200_OK)


class OrganizationTimeline(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_timeline.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        request_state, combine_same_as_name_only = prepare_request_state(request)
        request_state["hide_link"]="organization_timeline"
        o = Resource.nodes.get(uri=uri)
        org_serializer = OrganizationTimelineSerializer(o,context={"combine_same_as_name_only":combine_same_as_name_only})
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        uri_parts = elements_from_uri(o.uri)
        resp = Response({"timeline_serializer": org_serializer.data,
                            "org_data":org_data,
                            "request_state": request_state,
                            "uri_parts": uri_parts,
                            }, status=status.HTTP_200_OK)
        return resp


class RandomOrganization(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_linkages.html'

    def get(self, request):
        o = Organization.get_random()
        request_state, combine_same_as_name_only = prepare_request_state(request)
        vals = elements_from_uri(o.uri)
        org_serializer = OrganizationGraphSerializer(o)
        resp = Response({"data_serializer": org_serializer.data,
                            "org_data":vals,
                            "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp


class OrganizationByUri(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_linkages.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Resource.nodes.get(uri=uri)
        request_state, combine_same_as_name_only = prepare_request_state(request)
        request_state["hide_link"]="organization_linkages"
        org_serializer = OrganizationGraphSerializer(o,context=
                                    {"combine_same_as_name_only":combine_same_as_name_only})
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        uri_parts = elements_from_uri(o.uri)
        resp = Response({"data_serializer": org_serializer.data,
                            "org_data": org_data,
                            "uri_parts": uri_parts,
                            "request_state": request_state,
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


def prepare_request_state(request):
    combine_same_as_name_only = bool(int(request.GET.get("combine_same_as_name_only","1")))
    new_params = request.GET.dict()
    new_params["combine_same_as_name_only"] = int(not combine_same_as_name_only)
    new_url = f"{request.META['PATH_INFO']}?{urlencode(new_params)}"
    new_params["name_only_current_state"] = "Yes" if combine_same_as_name_only is True else "No"
    new_params["name_only_toggle_name"] = "Off" if combine_same_as_name_only is True else "On"
    new_params["name_only_toggle_url"] = new_url
    new_params["cache_last_updated"] = cache_last_updated_date()
    return new_params, combine_same_as_name_only
