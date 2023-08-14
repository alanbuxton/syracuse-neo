from django.shortcuts import render
from topics.models import Organization
from .serializers import OrganizationSerializer
from rest_framework import status
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.views import APIView
from rest_framework.response import Response

class OrganizationList(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'organizations.html'

    def get(self, request):
        orgs = Organization.nodes.order_by('?')[:5] # 5 random organizations
        serializer = OrganizationSerializer(orgs, many=True)
        resp = Response({"organizations":serializer.data}, status=status.HTTP_200_OK)
        return resp
