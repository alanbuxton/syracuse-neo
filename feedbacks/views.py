from django.shortcuts import render
from rest_framework import viewsets, status
from .models import Feedback
from .serializers import FeedbackSerializer
from urllib.parse import urlparse
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer, TemplateHTMLRenderer
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import action
import datetime
import re

class InteractiveFeedbackViewSet(viewsets.ModelViewSet):
    serializer_class = FeedbackSerializer
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'show_feedback.html'

    def create(self, request):
        data = request.data
        node_or_edge = data.get('node_or_edge')
        unique_id = data.get('idval')
        reason = data.get('reason')
        feedback_data = make_feedback_data(node_or_edge, unique_id, reason)
        source_page = request.headers.get("Referer")
        serializer = self.get_serializer(data=feedback_data)
        serializer.is_valid()
        if serializer.errors:
            return Response({"errors":serializer.errors, "source_page": source_page})
        self.perform_create(serializer)

        headers = self.get_success_headers(serializer.data)
        return Response({"feedback":serializer.data,
            "source_page":source_page}, status=status.HTTP_201_CREATED, headers=headers)


class UnprocessedFeedbacksViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Feedback.objects.filter(processed_at__isnull=True).order_by('id').all()
    serializer_class = FeedbackSerializer
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]

    @action(detail=False, methods=['post'])
    def mark_as_processed(self, request):
        ids = request.data['ids']
        ts = datetime.datetime.utcnow()
        feedbacks = Feedback.mark_as_processed(ids, ts)
        serializer = self.get_serializer(feedbacks, many=True)
        return Response(serializer.data)


def make_feedback_data(node_or_edge, unique_id, reason):
    if node_or_edge == 'node':
        doc_id = doc_id_from_uri(unique_id)
        parts = {"doc_id":doc_id,"source_node":unique_id,
            "target_node":None,"relationship":None}
    elif node_or_edge == 'edge':
        source, target, relationship = parts_from_edge_unique_id(unique_id)
        doc_id = doc_id_from_uri(source)
        parts = {"doc_id":doc_id,"source_node":source,"target_node":target,
                "relationship":relationship}
    parts["node_or_edge"] = node_or_edge
    parts["reason"] = reason
    return parts


def parts_from_edge_unique_id(unique_id):
    parts = re.findall(r"(http.+?)-(http.+?)-([A-Z_]+?)$",unique_id)
    if len(parts) == 0:
        return unique_id, None, None
    else:
        parts = parts[0]
        return parts

def doc_id_from_uri(uri):
    parts = urlparse(uri)
    if parts.netloc == '1145.am':
        doc_id = parts.path.split("/")[2] # Format is 1145.am/db/<doc_id>/etc
    else:
        doc_id = None
    return doc_id
