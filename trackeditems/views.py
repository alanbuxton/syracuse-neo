from django.shortcuts import redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from auth_extensions.anon_user_utils import IsAuthenticatedNotAnon
from rest_framework.views import APIView
# from rest_framework.generics import  ListCreateAPIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedItem
from topics.models import IndustryCluster
from rest_framework.response import Response
from rest_framework import status, viewsets
from .serializers import (ActivitySerializer,
    RecentsByGeoSerializer, RecentsBySourceSerializer, CountsSerializer,
    RecentsByIndustrySerializer, OrgIndGeoSerializer)
from topics.stats_helpers import get_cached_stats
from topics.activity_helpers import (
    get_activities_by_country_and_date_range,
    get_activities_by_industry_and_date_range,
    get_activities_by_source_and_date_range
    )
from datetime import datetime, timezone, timedelta, date
from topics.views import prepare_request_state
from .notification_helpers import recents_by_user_min_max_date
from topics.industry_geo import country_admin1_full_name

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
        resp = Response({"tracked_items":serialized.data},
                        status=status.HTTP_200_OK)
        return resp

    def post(self, request):
        payload = request.POST
        trackables = get_entities_to_track(payload, payload["search_str"])
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
                            "geo_name": geo_name,
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
                            "source_name": industry.longest_representative_doc,
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
                            "source_name": source_name,
                            "request_state": request_state,
                             }, status=status.HTTP_200_OK)
        return resp


class ActivityStats(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'activity_stats.html'

    def get(self, request):
        max_date, counts, recents_by_geo, recents_by_source, recents_by_industry = get_cached_stats()
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


def min_and_max_date(get_params):
    min_date = get_params.get("min_date")
    if isinstance(min_date, str):
        min_date = datetime.fromisoformat(min_date)
    max_date = get_params.get("max_date",datetime.now(tz=timezone.utc))
    if isinstance(max_date, str):
        max_date = datetime.fromisoformat(max_date)
    if max_date is not None and min_date is None:
        min_date = max_date - timedelta(days=7)
    return min_date, max_date

def get_entities_to_track(params_dict, search_str):
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

    return select_alls + specific_orgs_to_keep_cleaned
