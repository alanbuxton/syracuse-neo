from django.db import models

# Create your models here.

class Feedback(models.Model):

    node_or_edge = models.TextField()
    doc_id = models.BigIntegerField()
    source_node = models.TextField()
    target_node = models.TextField(blank=True, null=True) # only needed for edges
    relationship = models.TextField(blank=True, null=True) # only needed for edges
    reason = models.TextField()
    processed_at = models.DateTimeField(blank=True, null=True)

    @staticmethod
    def mark_as_processed(ids,ts):
        records = Feedback.objects.filter(id__in=ids)
        for record in records:
            record.processed_at = ts
            record.save()
        return records
