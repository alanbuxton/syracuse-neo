from django.shortcuts import render, redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedOrganization, TrackedIndustryGeo
from rest_framework.response import Response
from rest_framework import status, viewsets
from .serializers import (TrackedOrganizationSerializer,
    TrackedOrganizationModelSerializer, ActivitySerializer,
    RecentsByGeoSerializer, RecentsBySourceSerializer, CountsSerializer,
    TrackedIndustryGeoModelSerializer)
from topics.model_queries import (get_activities_for_serializer_by_country_and_date_range,
    get_cached_stats, get_activities_by_date_range_for_api,
    get_activities_for_serializer_by_source_and_date_range,
    get_activities_by_date_range_industry_geo_for_api)
from datetime import datetime, timezone, timedelta, date
from topics.geo_utils import country_and_region_code_to_name
from topics.views import prepare_request_state
from topics.serializers import date_from_str

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
        tracked_industry_geos = TrackedIndustryGeo.print_friendly_by_user(request.user)
        source_page = request.headers.get("Referer")
        request_state, _ = prepare_request_state(request)
        resp = Response({"tracked_orgs":tracked_org_serializer.data,"source_page":source_page,
                            "tracked_industry_geos":tracked_industry_geos,
                            "request_state": request_state,
                            },status=status.HTTP_200_OK)
        return resp

class GeoActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        geo_code = request.GET.get("geo_code")
        request_state, combine_same_as_name_only = prepare_request_state(request)
        if request_state["cache_last_updated"] is not None:
            matching_activity_orgs = get_activities_for_serializer_by_country_and_date_range(geo_code,min_date,max_date,limit=20,combine_same_as_name_only=False)
            geo_name = country_and_region_code_to_name(geo_code)
        else:
            matching_activity_orgs = []
            geo_name = ''
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                            "geo_name": geo_name,
                            "request_state": request_state,
                            }, status=status.HTTP_200_OK)
        return resp

class SourceActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'

    def get(self, request):
        min_date, max_date = min_and_max_date(request.GET)
        source_name = request.GET.get("source_name")
        request_state, combine_same_as_name_only = prepare_request_state(request)
        if request_state["cache_last_updated"] is not None:
            matching_activity_orgs =  get_activities_for_serializer_by_source_and_date_range(source_name, min_date, max_date, limit=20)
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
        max_date, counts, recents_by_geo, recents_by_source = get_cached_stats()
        recents_by_geo_serializer = RecentsByGeoSerializer(recents_by_geo, many=True)
        recents_by_source_serializer = RecentsBySourceSerializer(recents_by_source, many=True)
        counts_serializer = CountsSerializer(counts, many=True)
        request_state, combine_same_as_name_only = prepare_request_state(request)
        resp = Response({"recents_by_geo":recents_by_geo_serializer.data,"counts":counts_serializer.data,
                        "recents_by_source":recents_by_source_serializer.data,
                            "max_date":max_date,
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
        org_uris = TrackedOrganization.trackable_uris_by_user(user)
        matching_activity_orgs = get_activities_by_date_range_for_api(min_date,
                        uri_or_list=org_uris, max_date=max_date, combine_same_as_name_only=True)
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        request_state, combine_same_as_name_only = prepare_request_state(request)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                        "request_state": request_state,
                        }, status=status.HTTP_200_OK)
        return resp

class ActivitiesByUriViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = ActivitySerializer

    def get_queryset(self):
        uri = self.request.GET.get('uri')
        min_date = self.request.GET.get('min_date')
        results = get_activities_by_date_range_for_api(min_date,uri_or_list=[uri])
        return results
    
class ActivitiesByIndustryRegionViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = ActivitySerializer

    def get_queryset(self):
        industry_id = self.request.GET.get('industry_id')
        geo_code = self.request.GET.get('region_code')
        if industry_id is None and geo_code is None:
            raise ValueError("Must provide industry id or geo code (or both)")
        min_date = self.request.GET.get('min_date')
        min_date = date_from_str(min_date)
        if min_date is None:
            min_date = date.today() - timedelta(days=7)
        max_date = self.request.GET.get('max_date')
        max_date = date_from_str(max_date)
        if max_date is None:
            max_date = date.today()
        acts = get_activities_by_date_range_industry_geo_for_api(min_date, max_date,geo_code,industry_id)
        return acts
    

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
