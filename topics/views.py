from rest_framework.renderers import TemplateHTMLRenderer, JSONRenderer
from rest_framework.views import APIView
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from django.shortcuts import redirect
from topics.models import Organization, Resource, IndustryCluster
from topics.models.model_helpers import similar_organizations
from .serializers import (OrganizationGraphSerializer, OrganizationWithCountsSerializer,
    NameSearchSerializer, OrganizationSerializer,
    IndustrySerializer,OrganizationTimelineSerializer,
    ResourceSerializer, FamilyTreeSerializer, IndustryClusterSerializer,
    OrgsByIndustryGeoSerializer)
from rest_framework import status, viewsets
from datetime import date, timedelta
from topics.stats_helpers import cached_activity_stats_last_updated_date
from urllib.parse import urlencode
from syracuse.settings import MOTD
from integration.models import DataImport
from topics.faq import FAQ
from itertools import islice
from .industry_geo import country_admin1_full_name
from .industry_geo.orgs_by_industry_geo import combined_industry_geo_results, orgs_by_industry_cluster_and_geo
import re
import json
from .util import elements_from_uri, geo_to_country_admin1


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


class Index(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'index.html'

    def get(self,request):
        params = request.query_params
        org_name = params.get("name")
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
        else:
            orgs = Organization.randomized_active_nodes(10,min_date=min_date_for_article_counts)
            org_list = OrganizationWithCountsSerializer(orgs, many=True)
            org_search = NameSearchSerializer({"name":org_name})
            search_type = 'random'
            search_term = None
            num_hits = 0
        last_updated = DataImport.latest_import_ts()
        industry_search = IndustrySerializer()

        resp = Response({"organizations":org_list.data,
                        "search_serializer": org_search,
                        "search_term": search_term,
                        "num_hits": num_hits,
                        "industry_search": industry_search,
                        # "geo_search": geo_search,
                        "search_type": search_type,
                        "motd": MOTD,
                        "last_updated": last_updated,
                        # "search_industry_name": industry_name,
                        # "search_geo_code": selected_geo,
                        "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp

class ShowResource(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'show_resource.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

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
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

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
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

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

    def get(self, request, **kwargs):
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

class IndustryGeoFinderReview(ListCreateAPIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'industry_geo_finder_review.html'   

    def post(self, request):
        search_str = request.POST.get('searchStr')
        all_industry_ids = request.POST['allIndustryIDs']
        all_industry_ids = json.loads(all_industry_ids) 
        selected_cells = request.POST['selectedIndividualCells']
        indiv_cells = [row_col_data_to_tuple(x) for x in json.loads(selected_cells)]
        indiv_cells = remove_not_needed_admin1s_from_individual_cells(all_industry_ids,indiv_cells)
        selected_rows = request.POST['selectedRows']
        industry_ids = [row_from_request_post_data(x) for x in json.loads(selected_rows) if x != search_str]
        selected_columns = request.POST['selectedColumns']
        geo_codes = [col_from_request_post_data(x) for x in json.loads(selected_columns)]
        geo_codes = remove_not_needed_admin1s(geo_codes)
        request_state, _ = prepare_request_state(request)

        table_data = OrgsByIndustryGeoSerializer({
            "all_industry_ids": all_industry_ids,
            "industry_ids":industry_ids,
            "search_str": search_str,
            "geo_codes": geo_codes,
            "indiv_cells": indiv_cells,
            "search_str_in_all_geos": search_str in selected_rows,
        })
        
        resp = Response({"table_data":table_data.data,"search_str":search_str,
                         "all_industry_ids": all_industry_ids,
                         "request_state": request_state}, status=status.HTTP_200_OK)
        return resp
    
def remove_not_needed_admin1s_from_individual_cells(all_industry_ids, cells):
    cells_to_keep_full = []
    for industry_id in (all_industry_ids + ["search_str"]):
        relevant_cells = [(x,y) for x,y in cells if x == str(industry_id)]
        relevant_geos = [x[1] for x in relevant_cells]
        geos_to_keep = remove_not_needed_admin1s(relevant_geos)
        industry_cells_to_keep = [(x,y) for x,y in relevant_cells if y in geos_to_keep]
        cells_to_keep_full.extend(industry_cells_to_keep)
    return cells_to_keep_full
        
def remove_not_needed_admin1s(geo_codes):
    geo_codes_as_set = set(geo_codes)
    codes_with_admin1 = filter( lambda x: len(x) > 4, geo_codes)
    for code_with_admin1 in codes_with_admin1:
        country = code_with_admin1[:2]
        if country in geo_codes_as_set:
            geo_codes_as_set.remove(code_with_admin1)
    return list(geo_codes_as_set)
            
def row_col_data_to_tuple(val):
    topic_id = row_from_request_post_data(val)
    geo_code = col_from_request_post_data(val)
    return (topic_id, geo_code)

def row_from_request_post_data(val):
    val = re.search("row-(.+?)(?:\b|$|#)",val)
    if val:
        return val.groups()[0]
    else:
        return None

def col_from_request_post_data(val):
    val = re.search("col-(.+?)(?:\b|$|#)",val)
    if val:
        return val.groups()[0]
    else:
        return None 
    

class SimilarOrganizations(APIView):
    renderer_classes = [TemplateHTMLRenderer, JSONRenderer]
    template_name = 'similar_organizations.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    
    def get(self, request, **kwargs):
        uri = f"https://{kwargs['domain']}/{kwargs['path']}/{kwargs['doc_id']}/{kwargs['name']}"
        o = Resource.self_or_ultimate_target_node(uri)
        assert isinstance(o, Organization)
        org = OrganizationSerializer(o)
        similar = similar_organizations(o)
        similar_ind_clusters = {}
        for k,v in similar["industry_cluster"].items():
            similar_ind_clusters[k] = sorted(OrganizationSerializer(v, many=True).data, key=lambda x: x["best_name"])
        similar_ind_text = sorted(OrganizationSerializer(similar["industry_text"],many=True).data, key=lambda x: x["best_name"])
        request_state, _ = prepare_request_state(request)
        request_state["hide_link"]="similar_organizations"
        return Response({"organizations_by_industry_cluster": similar_ind_clusters,
                         "organizations_by_industry_text": similar_ind_text,
                         "org": org.data,
                         "request_state": request_state}, status=status.HTTP_200_OK)
    
class IndustryGeoFinder(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'industry_geo_finder.html'

    def get(self, request):
        industry_search_str = request.GET['industry']
        headers, ind_cluster_rows, text_row  = combined_industry_geo_results(industry_search_str) 
        request_state, _ = prepare_request_state(request)
        resp = Response({"table_body": ind_cluster_rows,
                         "text_row": text_row,
                         "table_header": headers,
                         "search_term": industry_search_str,
                         "industry_ids": json.dumps([x['industry_id'] for x in ind_cluster_rows]),
                         "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp


class IndustryGeoOrgsView(APIView):
    renderer_classes = [TemplateHTMLRenderer, JSONRenderer]
    template_name = 'industry_geo_orgs_list.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get(self, request):
        geo_code = request.GET.get("geo_code")
        industry_id = request.GET.get("industry_id")
        industry_cluster = IndustryCluster.get_by_industry_id(industry_id) 
        industry_cluster_uri = industry_cluster.uri if industry_cluster else None
        cc, adm1 = geo_to_country_admin1(geo_code)
        org_uris = orgs_by_industry_cluster_and_geo(industry_cluster_uri, industry_id, cc, adm1)
        ind_name = industry_cluster.best_name if industry_cluster else None
        geo_name = country_admin1_full_name(geo_code)
        organizations = []
        found_uris = set()
        for org_uri in org_uris:
            organization = Organization.self_or_ultimate_target_node(org_uri)
            if organization.uri in found_uris:
                continue
            found_uris.add(organization.uri)
            serialized_org = organization.serialize()
            serialized_org["splitted_uri"] = elements_from_uri(organization.uri)
            organizations.append(serialized_org)
            
        request_state, _ = prepare_request_state(request)
        resp = Response({"organizations": organizations,
                        "request_state": request_state,
                        "industry_geo_str": industry_geo_search_str(ind_name, geo_name)
                        }, status=status.HTTP_200_OK)   
        return resp     

def industry_geo_search_str(industry, geo):
    industry_str = "all industries" if industry is None or industry.strip() == '' else industry
    geo_str = "all locations" if geo is None or geo.strip() == '' else geo
    if geo_str.split()[0].lower() == 'united':
        in_str = "in the"
    else:
        in_str = "in"
    return f"<b>{industry_str.title()}</b> {in_str} <b>{geo_str}</b>"


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
    request_state["cache_last_updated"] = cached_activity_stats_last_updated_date()
    request_state["current_page_no_qs"] = current_path
    return request_state, combine_same_as_name_only
