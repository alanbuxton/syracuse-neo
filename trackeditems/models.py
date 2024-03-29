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
        return Organization.get_best_name_by_uri(self.organization_uri)

    @property
    def organization_or_merged_uri(self):
        org = Organization.get_by_uri_or_merged_uri(self.organization_uri)
        if org is not None:
            return org.uri

    @staticmethod
    def by_user(user):
        return TrackedOrganization.objects.filter(user=user)


class ActivityNotification(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    max_date = models.DateTimeField(null=False)
    num_activities = models.IntegerField(null=False)
    sent_at = models.DateTimeField()

    @staticmethod
    def most_recent(user):
        qs = ActivityNotification.objects.filter(user=user).order_by("-max_date")[:1]
        if len(qs) == 0:
            return None
        return qs[0].sent_at


def get_notifiable_users():
    '''
        Returns a distinct list of trackable organizations
    '''
    distinct_tracked_orgs = TrackedOrganization.objects.order_by("user").distinct("user")
    return distinct_tracked_orgs
