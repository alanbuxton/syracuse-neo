from django.db import models
from django.contrib.auth import get_user_model
from django.db.models.functions import Lower
from topics.models import Organization
import logging
logger = logging.getLogger(__name__)

class TrackedOrganization(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    organization_uri = models.URLField(null=False)

    class Meta:
        constraints = [
            models.UniqueConstraint("user", Lower("organization_uri").desc(), name="trackeditems_unique_user_organization_uri")
        ]
        ordering = ['organization_uri']

    @property
    def organization_name(self):
        return Organization.get_longest_name_by_uri(self.organization_uri)

    @staticmethod
    def uris_by_user(user):
        objs = TrackedOrganization.objects.filter(user=user)
        return [x.organization_uri for x in objs]

    @staticmethod
    def orgs_by_user(user):
        uris = TrackedOrganization.by_user(user)
        return Organization.by_uris(uris)


def get_notifiable_users():
    '''
        Returns a distinct list of trackable organizations
    '''
    distinct_tracked_orgs = TrackedOrganization.objects.order_by("user").distinct("user")
    return distinct_tracked_orgs
