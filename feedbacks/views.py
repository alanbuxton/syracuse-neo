from django.shortcuts import render
from rest_framework import viewsets, status
from .models import Feedback
from .serializers import FeedbackSerializer
from urllib.parse import urlparse
from rest_framework.response import Response
from rest_framework.renderers import TemplateHTMLRenderer, JSONRenderer
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import action
from datetime import datetime, timezone
import re
import logging
logger = logging.getLogger(__name__)

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


class MarkAsProcessedViewSet(viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated]
    authentication_classes = [SessionAuthentication, TokenAuthentication]
    serializer_class = FeedbackSerializer
    renderer_classes = [JSONRenderer]

    def create(self, request):
        ids = request.data.get('ids') # could be e.g. [1,2,3] or ["1","2","3"]
        if ids is None:
            logger.error(f"mark as processed sent but without any ids")
            return Response([])
        ids = map(int, ids)
        ts = datetime.now(tz=timezone.utc)
        feedbacks = Feedback.mark_as_processed(ids, ts)
        serializer = FeedbackSerializer(feedbacks, many=True)
        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_200_OK, headers=headers)


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
