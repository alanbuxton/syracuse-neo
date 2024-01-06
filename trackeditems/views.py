from django.shortcuts import render, redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedOrganization
from rest_framework.response import Response
from rest_framework import status, viewsets
from .serializers import (TrackedOrganizationSerializer,
    TrackedOrganizationModelSerializer, ActivitySerializer,
    RecentsSerializer, CountsSerializer)
import json
from topics.model_queries import get_activities_by_date_range_for_api
from .date_helpers import days_ago
from topics.model_queries import get_relevant_orgs_for_country, get_stats
from datetime import datetime, timezone, date
from topics.geo_utils import COUNTRY_CODES

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
        source_page = request.headers.get("Referer")
        resp = Response({"tracked_orgs":tracked_org_serializer.data,"source_page":source_page},status=status.HTTP_200_OK)
        return resp

class GeneralActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'

    def get(self, request):
        min_date = request.GET.get("min_date",days_ago(5))
        if isinstance(min_date, str):
            min_date = datetime.fromisoformat(min_date)
        max_date = request.GET.get("max_date",datetime.now(tz=timezone.utc))
        country_code = request.GET.get("country_code")
        relevant_orgs = get_relevant_orgs_for_country(country_code)
        relevant_uris = [x.uri for x in relevant_orgs]
        matching_activity_orgs = get_activities_by_date_range_for_api(min_date, relevant_uris,
                                    max_date, limit=20, include_same_as = False)
        country_name = COUNTRY_CODES.get(country_code)
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date,
                            "country_name": country_name }, status=status.HTTP_200_OK)
        return resp

class ActivityStats(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'activity_stats.html'

    def get(self, request):
        max_date = request.GET.get("max_date",date.today())
        counts, recents = get_stats(max_date)
        recents_serializer = RecentsSerializer(recents, many=True)
        counts_serializer = CountsSerializer(counts, many=True)
        resp = Response({"recents":recents_serializer.data,"counts":counts_serializer.data,
                            "max_date":max_date}, status=status.HTTP_200_OK)
        return resp

class ActivitiesView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'tracked_activities.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get(self, request):
        min_date = request.GET.get("min_date",days_ago(5))
        if isinstance(min_date, str):
            min_date = datetime.fromisoformat(min_date)
        max_date = request.GET.get("max_date",datetime.now(tz=timezone.utc))
        user = request.user
        orgs = TrackedOrganization.uris_by_user(user)
        matching_activity_orgs = get_activities_by_date_range_for_api(min_date, orgs, max_date, include_same_as=True)
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date}, status=status.HTTP_200_OK)
        return resp

class ActivitiesViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = ActivitySerializer

    def get_queryset(self):
        uri = self.request.GET.get('uri')
        min_date = self.request.GET.get('min_date')
        results = get_activities_by_date_range_for_api(min_date,[uri])
        return results
