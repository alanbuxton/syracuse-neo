from rest_framework import serializers
from .models import Feedback

class FeedbackSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feedback
        fields = ['id', 'node_or_edge', 'doc_id', 'source_node', 'target_node',
                    'relationship', 'reason', 'processed_at']
