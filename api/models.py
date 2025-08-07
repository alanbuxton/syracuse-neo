from django.db import models
from django.contrib.auth.models import User
from django.db.models import JSONField  # Requires Django 3.1+
from django.contrib.postgres.indexes import GinIndex

class APIRequestLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    path = models.TextField()
    method = models.TextField()
    status_code = models.IntegerField()
    duration = models.FloatField()
    ip = models.GenericIPAddressField(null=True, blank=True)
    query_params = JSONField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"[{self.timestamp}] {self.user} {self.method} {self.path} ({self.status_code})"
    
    class Meta:
        indexes = [
            GinIndex(fields=['query_params']), # to support e.g. APIRequestLog.objects.filter(query_params__region__contains="eu")
        ]