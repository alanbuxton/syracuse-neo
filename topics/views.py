from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from django.shortcuts import redirect
from topics.models import Organization, Resource, IndustryCluster
from .serializers import (OrganizationGraphSerializer, OrganizationWithCountsSerializer,
    NameSearchSerializer, GeoSerializer, 
    IndustrySerializer,OrganizationTimelineSerializer,
    ResourceSerializer, FamilyTreeSerializer,
    CountryRegionSerializer, IndustryClusterSerializer)
from rest_framework import status, viewsets
from datetime import date, timedelta
from precalculator.models import cache_last_updated_date
from urllib.parse import urlparse, urlencode
from syracuse.settings import MOTD
from integration.models import DataImport
from topics.faq import FAQ
from itertools import islice
from topics.geo_utils import get_geo_data
from topics.industry_geo_helpers import prepare_industry_table

import logging
logger = logging.getLogger(__name__)


class About(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = "p/about.html"

    def get(self,request):
        return Response({"faqs": FAQ}, status=status.HTTP_200_OK)
    

class IndustriesViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = IndustryClusterSerializer

    def get_queryset(self):
        return IndustryCluster.for_external_api()

class CountriesAndRegionsViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = CountryRegionSerializer 

    def get_queryset(self):
        _, vals, _ = get_geo_data()
        return [
            {"country_name":x,
             "region_name": y,
             "country_region_code": z} for x,y,z in vals
        ]

    
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
        request_state, combine_same_as_name_only = prepare_request_state(request)
        min_date_for_article_counts = date.today() - timedelta(days = 365 * 2)

        if org_name:
            orgs = Organization.find_by_name(org_name, combine_same_as_name_only, 
                                             min_date=min_date_for_article_counts)
            orgs = sorted(orgs, key=lambda x: x[1], reverse=True)
            num_hits = len(orgs)
            if len(orgs) > 20:
                orgs = islice(orgs,20)
            org_list = OrganizationWithCountsSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            search_type = 'org_name'
            search_term = org_name
        elif selected_geo or industry:
            orgs = Organization.by_industry_and_or_geo(industry,selected_geo,
                                                       min_date=min_date_for_article_counts)
            num_hits = len(orgs)
            if len(orgs) > 20:
                orgs = islice(orgs,20)
            orgs = sorted(orgs, key=lambda x: x[1], reverse=True)
            org_list = OrganizationWithCountsSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":""})
            search_type = 'combined_search'
            search_term = industry_geo_search_str(industry_name, selected_geo_name)
        else:
            orgs = Organization.randomized_active_nodes(10,min_date=min_date_for_article_counts)
            org_list = OrganizationWithCountsSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            search_type = 'random'
            search_term = None
            num_hits = 0
        last_updated = DataImport.latest_import_ts()
        geo_search = GeoSerializer()
        industry_search = IndustrySerializer({"industry":industry_name})

        resp = Response({"organizations":org_list.data,
                        "search_serializer": org_search,
                        "search_term": search_term,
                        "num_hits": num_hits,
                        "industry_search": industry_search,
                        "geo_search": geo_search,
                        "search_type": search_type,
                        "motd": MOTD,
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
        request_state, _ = prepare_request_state(request)
        doc_id = kwargs.get("doc_id")
        if doc_id is not None:
            uri = f"{uri}/{doc_id}"
        uri = f"{uri}/{kwargs['name']}"
        r = Resource.nodes.get(uri=uri)
        if r is None:
            raise ValueError(f"Couldn't find Resource with uri {uri}")
        if isinstance(r, Organization):
            resp = redirect("organization-linkages",**kwargs)
            if len(request.GET) > 0:
                resp['Location'] = resp['Location'] + "?" + urlencode(request.GET) # https://stackoverflow.com/a/41875812/7414500
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
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name},**o.serialize_no_none()}
        uri_parts = elements_from_uri(o.uri)
        relationships = request_state["qs_params"].get("rels","buyer,vendor")
        source_str = request_state["qs_params"].get("sources","_core")
        earliest_str = request_state["qs_params"].get("earliest_date","")
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":combine_same_as_name_only,
                                                                "relationship_str":relationships,
                                                                "source_str":source_str,
                                                                "earliest_str":earliest_str})

        relationship_vals = set(relationships.split(","))  
        relationship_link_data = self.create_relationship_links(request_state, relationship_vals)    
        nodes_edges_data = nodes_edges.data
        request_state["document_sources"]=nodes_edges_data.pop("document_sources")
        request_state["earliest_doc_date"]=nodes_edges_data.pop("earliest_doc_date")
        return Response({"nodes_edges":nodes_edges_data,
                            "requested_uri": uri,
                            "org_data": org_data,
                            "uri_parts": uri_parts,
                            "relationship_link_data": relationship_link_data,
                            "request_state": request_state}, status=status.HTTP_200_OK)
    
    def create_relationship_links(self, request_state, rels):       
        options = [set(["buyer","vendor"]),
                    set(["investor"]),
                    set(["buyer","investor","vendor"])]
        names = ["Acquisitions","Investments","All"]
        idx = options.index(rels)
        selected_name = names[idx]
        next_vals = []
        for tmp_idx,(row_opts,row_name) in enumerate(zip(options,names)):
            if tmp_idx == idx:
                continue
            next_vals.append( {"name":row_name, "query_string_params": {**request_state["qs_params"],**{"rels":",".join(sorted(row_opts))}} } )
        return {"selected_name":selected_name, "next_vals": next_vals}            
            
class OrganizationTimeline(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organization_timeline.html'

    def get(self, request, *args, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        request_state, combine_same_as_name_only = prepare_request_state(request)
        request_state["hide_link"]="organization_timeline"
        o = Resource.nodes.get(uri=uri)
        source_str = request_state["qs_params"].get("sources","_core")
        earliest_str = request_state["qs_params"].get("earliest_date","")
        org_serializer = OrganizationTimelineSerializer(o, context={"combine_same_as_name_only":combine_same_as_name_only,
                                                                    "source_str":source_str,
                                                                    "earliest_str":earliest_str})
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        uri_parts = elements_from_uri(o.uri)
        org_serializer_data = org_serializer.data
        request_state["document_sources"] = org_serializer_data.pop("document_sources")
        request_state["earliest_doc_date"] = org_serializer_data.pop("earliest_doc_date")
        resp = Response({"timeline_serializer": org_serializer_data,
                            "org_data": org_data,
                            "request_state": request_state,
                            "uri_parts": uri_parts,
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
        source_str = request_state["qs_params"].get("sources","_core")
        earliest_str = request_state["qs_params"].get("earliest_date","")
        org_serializer = OrganizationGraphSerializer(o,context=
                                    {"combine_same_as_name_only":combine_same_as_name_only,
                                     "source_str":source_str,
                                     "earliest_str":earliest_str,
                                     })
        org_data = {**kwargs, **{"uri":o.uri,"source_node_name":o.best_name}}
        uri_parts = elements_from_uri(o.uri)
        org_serializer_data = org_serializer.data
        request_state["document_sources"]=org_serializer_data.pop("document_sources")
        request_state["earliest_doc_date"] = org_serializer_data.pop("earliest_doc_date") 
        resp = Response({"data_serializer": org_serializer_data,
                            "org_data": org_data,
                            "uri_parts": uri_parts,
                            "request_state": request_state,
                            }, status=status.HTTP_200_OK)
        return resp


class IndustryGeoFinder(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'industry_geo_finder.html'

    def get(self, request, *args, **kwargs):
        industry_search_str = kwargs['industry_search']
        
        industry_table_header, industry_table_body  = prepare_industry_table(industry_search_str) 
        
        request_state, _ = prepare_request_state(request)
        resp = Response({"table_body": industry_table_body,
                         "table_header": industry_table_header,
                         "search_term": industry_search_str,
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


def industry_geo_search_str(industry, geo):
    industry_str = "all industries" if industry is None or industry.strip() == '' else industry
    geo_str = "all locations" if geo is None or geo.strip() == '' else geo
    if geo_str.split()[0].lower() == 'united':
        in_str = "in the"
    else:
        in_str = "in"
    return f"<b>{industry_str.title()}</b> {in_str} <b>{geo_str.title()}</b>"


def prepare_request_state(request):
    combine_same_as_name_only = bool(int(request.GET.get("combine_same_as_name_only","1")))
    qs_params = request.GET.dict()
    toggle_combine_as_same_name_only = int(not combine_same_as_name_only)
    toggle_params = {**qs_params,**{"combine_same_as_name_only":toggle_combine_as_same_name_only}}
    current_path = request.META['PATH_INFO']
    new_url = f"{current_path}?{urlencode(toggle_params)}"
    request_state = {}
    request_state["qs_params"] = qs_params
    request_state["name_only_current_state"] = "Yes" if combine_same_as_name_only is True else "No"
    request_state["name_only_toggle_name"] = "Off" if combine_same_as_name_only is True else "On"
    request_state["name_only_toggle_url"] = new_url
    request_state["cache_last_updated"] = cache_last_updated_date()
    request_state["current_page_no_qs"] = current_path
    return request_state, combine_same_as_name_only
