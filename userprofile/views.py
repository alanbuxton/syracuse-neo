from django.shortcuts import render, redirect
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from .models import TrackedOrganization
from rest_framework.response import Response
from rest_framework import status
from .serializers import TrackedOrganizationSerializer
import json

class TrackedOrganizationView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'my_preferences.html'
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    def get_queryset(self):
        return TrackedOrganization.objects.filter(user=self.request.user)

    def post(self, request, **kwargs):
        new_orgs_as_string = request.data['tracked_organization_names']
        new_org_list = json.loads(new_orgs_as_string)
        for new_org in new_org_list:
            validated_data = {"organization_name":new_org, "user":request.user}
            _ = TrackedOrganizationSerializer().upsert(validated_data)
        return redirect('tracked-organizations')

    def get(self, request):
        tracked_orgs = self.get_queryset()
        tracked_org_serializer = TrackedOrganizationSerializer(tracked_orgs, many=True)
        source_page = request.headers.get("Referer")
        return Response({"tracked_orgs":tracked_org_serializer.data,"source_page":source_page},status=status.HTTP_200_OK)
