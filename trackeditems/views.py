from django.shortcuts import redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from auth_extensions.anon_user_utils import IsAuthenticatedNotAnon
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedItem
from topics.models import IndustryCluster
from topics.util import min_and_max_date
from rest_framework.response import Response
from rest_framework import status
from .serializers import (ActivitySerializer,
    RecentsByGeoSerializer, RecentsBySourceSerializer, CountsSerializer,
    RecentsByIndustrySerializer, OrgIndGeoSerializer, TrackedItemSerializer)
from topics.stats_helpers import get_cached_stats
from topics.activity_helpers import (
    get_activities_by_country_and_date_range,
    get_activities_by_industry_and_date_range,
    get_activities_by_source_and_date_range,
    get_activities_by_industry_geo_and_date_range,
)
from topics.views import prepare_request_state
from .notification_helpers import recents_by_user_min_max_date
from topics.industry_geo import country_admin1_full_name
import json
from django.shortcuts import get_object_or_404
from django.http import QueryDict
import logging
logger = logging.getLogger(__name__)

class TrackedOrgIndGeoView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_org_ind_geo.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    http_method_names = ['get', 'post']

    def get_queryset(self):
        return TrackedItem.trackable_by_user(user=self.request.user)
    
    def get(self, request):
        tracked_items = self.get_queryset()
        serialized = OrgIndGeoSerializer(tracked_items, many=True)
        request_state, _ = prepare_request_state(request)
        resp = Response({"tracked_items":serialized.data,
                         "request_state":request_state},
                        status=status.HTTP_200_OK)
        return resp

    def post(self, request):
        payload = request.POST
        # This endpoint can be called from two sources, each with different payload structure
        payload_for_trackables = standardize_payload(payload)
        all_industry_ids = json.loads(payload_for_trackables.get("all_industry_ids","[]"))
        trackables = get_entities_to_track(payload_for_trackables, 
                                           payload_for_trackables.get("search_str",""), 
                                           all_industry_ids)
        TrackedItem.update_or_create_for_user(request.user, trackables)
        return redirect('tracked-org-ind-geo')

class GeoActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        geo_code = request.GET.get("geo_code")
        request_state, _ = prepare_request_state(request)
        if request_state["cache_last_updated"] is not None:
            matching_activity_orgs = get_activities_by_country_and_date_range(geo_code,min_date,max_date,limit=20)
            geo_name = country_admin1_full_name(geo_code)
        else:
            matching_activity_orgs = []
            geo_name = ''
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                            "source_name": {
                                "from_str": "",
                                "industry_str": "",
                                "in_str": in_str_for_region_str(geo_name),
                                "region_str": geo_name,
                            },
                            "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp
    
class IndustryActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, TokenAuthentication]   

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        industry_id = request.GET.get("industry_id")
        industry = IndustryCluster.nodes.get_or_none(topicId=industry_id)
        request_state, _ = prepare_request_state(request)
        if request_state["cache_last_updated"] is not None:
            matching_activity_orgs = get_activities_by_industry_and_date_range(industry, min_date, max_date, limit=20)
        else:
            matching_activity_orgs = []
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                            "source_name": {
                                "from_str": "from",
                                "in_str" : "",
                                "region_str": "",
                                "industry_str": industry.longest_representative_doc,
                            },
                            "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp
    
class IndustryGeoActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, TokenAuthentication]   

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        industry_id = request.GET.get("industry_id")
        if industry_id is None or industry_id.strip() == '' or industry_id == 'None':
            industry = None
        else:
            industry = IndustryCluster.nodes.get_or_none(topicId=industry_id)
        geo_code = request.GET.get("geo_code")
        if geo_code == '':
            geo_code = None
        request_state, _ = prepare_request_state(request)
        matching_activity_orgs = get_activities_by_industry_geo_and_date_range(industry, geo_code, min_date, max_date, limit=20)
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                            "source_name": geo_industry_to_string(geo_code, industry),
                            "request_state": request_state,
                             }, status=status.HTTP_200_OK)
        return resp    

class SourceActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    
    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        source_name = request.GET.get("source_name")
        request_state, _ = prepare_request_state(request)
        if request_state["cache_last_updated"] is not None:
            matching_activity_orgs = get_activities_by_source_and_date_range(source_name, min_date, max_date, limit=20)
        else:
            matching_activity_orgs = []
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                            "source_name": {
                                "source_organization": source_name,
                            },
                            "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp


class ActivityStats(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'activity_stats.html'

    def get(self, request):
        max_sources = int(request.GET.get("max_sources","100"))
        max_date, counts, recents_by_geo, recents_by_source, recents_by_industry = get_cached_stats(max_sources=max_sources)
        recents_by_geo_serializer = RecentsByGeoSerializer(recents_by_geo, many=True)
        recents_by_source_serializer = RecentsBySourceSerializer(recents_by_source, many=True)
        counts_serializer = CountsSerializer(counts, many=True)
        request_state, _ = prepare_request_state(request)
        recents_by_industry_serializer = RecentsByIndustrySerializer(recents_by_industry,many=True)
        resp = Response({"recents_by_geo": recents_by_geo_serializer.data,
                         "counts": counts_serializer.data,
                         "recents_by_source": recents_by_source_serializer.data,
                         "recents_by_industry": recents_by_industry_serializer.data,
                         "max_date": max_date,
                         "request_state": request_state,
                         "sources_cnt": max_sources,
                        }, status=status.HTTP_200_OK)
        return resp


class ActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticatedNotAnon]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        user = request.user
        matching_activity_orgs, _ = recents_by_user_min_max_date(user, min_date, max_date)
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        request_state, _ = prepare_request_state(request)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                        "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp

def get_entities_to_track(params_dict, search_str, all_industry_ids):
    tracked_items = [TrackedItem.text_to_tracked_item_data(x,search_str) 
            for  x,y in params_dict.items() if x.startswith("track_") and y[0] == '1']
    select_alls = []
    specific_orgs = []
    for ti in tracked_items:
        select_alls.append(ti) if ti['organization_uri'] is None else specific_orgs.append(ti)
    
    def is_covered_by_select_all(tracked_item):
        industry_id = tracked_item['industry_id']
        industry_search_str = tracked_item['industry_search_str']
        region = tracked_item['region']
        for x in select_alls:
            if (industry_id == x['industry_id'] and
                industry_search_str == x['industry_search_str'] and
                region == x['region']):
                return True
        return False
    
    specific_orgs_to_keep = [x for x in specific_orgs if not is_covered_by_select_all(x)]
    def clean_specific_org(tracked_item):
        return {"organization_uri":tracked_item["organization_uri"],
                "trackable": tracked_item["trackable"],
                "industry_id": None, "industry_search_str": None, "region": None}
    specific_orgs_to_keep_cleaned = [clean_specific_org(x) for x in specific_orgs_to_keep]

    def add_industry_clusters_to_regional_tracked_items(tracked_item):
        if (tracked_item['region'] is not None) and (tracked_item['industry_id'] is None) and (tracked_item['organization_uri'] is None):
            return [{**tracked_item,**{"industry_id":x}} for x in all_industry_ids]
        else:
            return [tracked_item]
        
    def more_generic_version_exists(tracked_item):
        if tracked_item['industry_id'] is None or tracked_item['region'] is None or tracked_item['organization_uri'] is not None:
            return False
        for x in expanded_select_alls:
            if tracked_item['trackable'] != x['trackable']:
                continue
            if tracked_item['industry_id'] == x['industry_id'] and (
                x['region'] is None or (len(x['region']) == 2 and 
                    len(tracked_item['region']) > 2 and
                    tracked_items['region'][:2] == x['region'])):
                return True
            if x['industry_id'] is None and (
                tracked_item['region'] == x['region'] or (
                    len(x['region']) == 2 and len(tracked_item['region']) > 2 and
                    tracked_item['region'][:2] == x['region'])):
                return True            
        return False

    expanded_select_alls = []
    for select_all in select_alls:
        expanded_select_alls.extend(add_industry_clusters_to_regional_tracked_items(select_all))

    expanded_select_alls = [x for x in expanded_select_alls if not more_generic_version_exists(x)]
        
    return expanded_select_alls + specific_orgs_to_keep_cleaned

def geo_industry_to_string(geo_code, industry):
    region_str = "All Locations" if (geo_code is None or 
                                        geo_code.strip() == '') else country_admin1_full_name(geo_code)
    if isinstance(industry, IndustryCluster):
        industry_name = industry.longest_representative_doc
    else:
        industry_name = None
    industry_str = "All Industries" if (industry_name is None or industry_name.strip() == '') else industry_name
    in_str = in_str_for_region_str(region_str)
    return {"geo_code":geo_code,
            "from_str": "from",
            "region_str": region_str,
            "industry_name": industry_name,
            "industry_str": industry_str,
            "in_str": in_str,
            }   

def in_str_for_region_str(region_str):
    return "in the" if region_str.lower().startswith("united") else "in"


class ToggleTrackedItemView(APIView):
    authentication_classes = [SessionAuthentication] 
    permission_classes = [IsAuthenticatedNotAnon]

    def patch(self, request, item_id):
        item = get_object_or_404(TrackedItem, id=item_id)
        item.and_similar_orgs = not item.and_similar_orgs 
        item.save()
        serializer = TrackedItemSerializer(item)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
def standardize_payload(payload):
    '''
    If payload is in format from industry_geo_finder then convert it into format
    from industry_geo_finder_review 
    '''
    new_payload = QueryDict('',mutable=True)
    new_payload.update(payload)
    selected_rows = payload.get('selectedRows','[]')
    rows = json.loads(selected_rows)
    for row in rows:
        row_id = row.replace("row-","")
        new_payload[f"track_selectall_{row_id}"] = '1'

    selected_columns = payload.get('selectedColumns','[]')
    cols = json.loads(selected_columns)
    for col in cols:
        col_id = col.replace("col-","")
        new_payload[f"track_selectall_{col_id}"] = '1'

    selected_cells = payload.get('selectedIndividualCells','[]')
    cells = json.loads(selected_cells)
    for cell in cells:
        row,col = cell.split("#")
        row_id = row.replace("row-","")
        col_id = col.replace("col-","")
        new_payload[f"track_selectall_{row_id}_{col_id}"] = '1'

    new_payload["all_industry_ids"] = payload["allIndustryIDs"]

    return new_payload