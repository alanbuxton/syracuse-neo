from django.db import models
from django.contrib.auth import get_user_model
from django.db.models.functions import Lower

class TrackedOrganization(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    organization_name = models.TextField()

    class Meta:
        constraints = [
            models.UniqueConstraint("user", Lower("organization_name").asc(), name="unique_user_organization_name")
        ]
        ordering = ['organization_name']

    @classmethod
    def get_case_insensitive(cls, data):
        res = cls.objects.filter(user_id=data["user"].id, organization_name__iexact=data["organization_name"])
        if len(res) == 0:
            return None
        else:
            return res[0]
