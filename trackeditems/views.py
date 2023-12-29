from django.shortcuts import render, redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedOrganization
from rest_framework.response import Response
from rest_framework import status, viewsets
from .serializers import TrackedOrganizationSerializer, ActivitySerializer
import json
from topics.model_queries import get_activities_by_date_range_for_api
from .helpers import days_ago
from datetime import datetime, timezone

class TrackedOrganizationView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'preferences.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get_queryset(self):
        return TrackedOrganization.objects.filter(user=self.request.user)

    def post(self, request, **kwargs):
        new_orgs_as_string = request.data['tracked_organization_names']
        new_org_list = json.loads(new_orgs_as_string)
        existing_orgs = TrackedOrganization.by_user(request.user)
        orgs_so_far = len(existing_orgs)
        for new_org in new_org_list:
            if orgs_so_far <= 20:
                validated_data = {"organization_name":new_org, "user":request.user}
                _ = TrackedOrganizationSerializer().upsert(validated_data)
                orgs_so_far += 1
        return redirect('tracked-organizations')

    def get(self, request):
        tracked_orgs = self.get_queryset()
        tracked_org_serializer = TrackedOrganizationSerializer(tracked_orgs, many=True)
        source_page = request.headers.get("Referer")
        return Response({"tracked_orgs":tracked_org_serializer.data,"source_page":source_page},status=status.HTTP_200_OK)

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
        keywords = TrackedOrganization.by_user(user)
        matching_activity_orgs = get_activities_by_date_range_for_api(min_date, keywords, max_date)
        serializer = ActivitySerializer(matching_activity_orgs, many=True)
        resp = Response({"activities":serializer.data,"min_date":min_date,"max_date":max_date}, status=status.HTTP_200_OK)
        return resp

class ActivitiesViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = ActivitySerializer

    def get_queryset(self):
        name_list = self.request.GET.get('name_list')
        min_date = self.request.GET.get('min_date')
        results = get_activities_by_date_range_for_api(min_date,[name_list])
        return results
