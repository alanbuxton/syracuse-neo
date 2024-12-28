from django.shortcuts import redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedOrganization, TrackedIndustryGeo
from topics.models import IndustryCluster
from rest_framework.response import Response
from rest_framework import status, viewsets
from .serializers import (TrackedOrganizationSerializer,
    TrackedOrganizationModelSerializer, ActivitySerializer,
    RecentsByGeoSerializer, RecentsBySourceSerializer, CountsSerializer,
    TrackedIndustryGeoModelSerializer, RecentsByIndustrySerializer,
    TrackedIndustryGeoSerializer)
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

class TrackedIndustryGeoView(APIView):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def post(self, request):
        industry_name = request.data['tracked_industry_name']
        geo_code = request.data['tracked_geo_code']
        if industry_name == '' and geo_code == '':
            return redirect('tracked-organizations')
        existing_industry_geos = TrackedIndustryGeo.items_by_user(self.request.user)
        if len(existing_industry_geos) <= 20 and (industry_name,geo_code) not in existing_industry_geos:
            data = {"user":self.request.user, "industry_name":industry_name,"geo_code":geo_code}
            _ = TrackedIndustryGeoModelSerializer().create(data)
        return redirect('tracked-organizations')

class TrackedOrganizationView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'preferences.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get_queryset(self):
        return TrackedOrganization.objects.filter(user=self.request.user)

    def post(self, request, **kwargs):
        uri = request.data['tracked_organization_uri']
        existing_orgs = TrackedOrganization.uris_by_user(request.user)
        orgs_so_far = len(existing_orgs)
        if orgs_so_far <= 20 and uri not in existing_orgs:
            data = {"organization_uri":uri, "user":request.user}
            _ = TrackedOrganizationModelSerializer().create(data)
        return redirect('tracked-organizations')

    def get(self, request):
        tracked_orgs = self.get_queryset()
        tracked_org_serializer = TrackedOrganizationSerializer(tracked_orgs, many=True)
        tracked_industry_geos = TrackedIndustryGeo.by_user(request.user)
        tracked_industry_geo_serializer = TrackedIndustryGeoSerializer(tracked_industry_geos, many=True)
        source_page = request.headers.get("Referer")
        request_state, _ = prepare_request_state(request)
        resp = Response({"tracked_orgs":tracked_org_serializer.data,"source_page":source_page,
                            "tracked_industry_geos":tracked_industry_geo_serializer.data,
                            "request_state": request_state,
                            },status=status.HTTP_200_OK)
        return resp

class GeoActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticated]
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
    permission_classes = [IsAuthenticated]
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
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    
    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        source_name = request.GET.get("source_name")
        request_state, _ = prepare_request_state(request)
        if request_state["cache_last_updated"] is not None:
            matching_activity_orgs =  get_activities_by_source_and_date_range(source_name, min_date, max_date, limit=20)
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
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        user = request.user
        matching_activity_orgs, _, _ = recents_by_user_min_max_date(user, min_date, max_date)
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
