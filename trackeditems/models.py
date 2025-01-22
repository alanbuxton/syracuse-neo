from django.db import models
from django.contrib.auth import get_user_model
from django.db.models.functions import Lower
from topics.models import Organization
import re
import logging
logger = logging.getLogger(__name__)

class TrackedItem(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    industry_id = models.IntegerField() # matches neo4j topicId
    industry_search_str = models.TextField()
    region = models.TextField() # e.g. US-CA, or Europe
    organization_uri = models.URLField()
    trackable = models.BooleanField(default=True)

    class Meta:
        constraints = [
            models.UniqueConstraint("user","industry_id",Lower("region"), name= "trackeditem_unique_user_industry_region"),
            models.UniqueConstraint("user", Lower("organization_uri"), name="trackeditem_unique_user_organization_uri"),
            models.UniqueConstraint("user", Lower("industry_search_str"),Lower("region"), name="trackeditem_unique_user_search_str_region"),
        ]

    @staticmethod
    def text_to_tracked_item_data(text,search_str=""):
        splitted = re.split(r"_(https://.+)$",text)
        organization_uri = splitted[1] if len(splitted) > 1 else None
        elements = splitted[0].split("_")
        if "select" in elements[0]:
            trackable = False if "unselect" in elements[0] else True
            elements.pop(0)
        else:
            trackable = True

        industry_id = None
        industry_search_str = None
        try:
            industry_id = int(elements[0])
            elements.pop(0)
        except ValueError:
            if elements[0] == "searchstr":
                industry_search_str = search_str
                elements.pop(0)
        region = elements[0] if len(elements) > 0 else None
        return {"industry_id": industry_id,
                "industry_search_str":industry_search_str,
                "region": region,
                "organization_uri": organization_uri,
                "trackable": trackable,
                }

class TrackedIndustryGeo(models.Model):
    '''
    Deprecated
    '''
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    industry_name = models.TextField()
    geo_code = models.TextField() # e.g. US-CA

    class Meta:
        constraints = [
            models.UniqueConstraint("user",Lower("industry_name"),Lower("geo_code"), name= "trackedgeoindustry_unique_user_industry_geo")
        ]
        ordering = ['industry_name','geo_code']

    @staticmethod
    def by_user(user):
        return TrackedIndustryGeo.objects.filter(user=user)

class TrackedOrganization(models.Model):
    '''
    Deprecated
    '''
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
        org = Organization.self_or_ultimate_target_node(self.organization_uri)
        if org is not None:
            return org.uri

    @staticmethod
    def by_user(user):
        return TrackedOrganization.objects.filter(user=user)

    @staticmethod
    def uris_by_user(user):
        orgs = TrackedOrganization.by_user(user)
        return [x.organization_uri for x in orgs]

    @staticmethod
    def trackable_uris_by_user(user):
        tracked_orgs = TrackedOrganization.by_user(user)
        org_uris = [x.organization_or_merged_uri for x in tracked_orgs]
        return org_uris


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
        return qs[0].max_date


def get_notifiable_users():
    '''
        Returns a distinct list of trackable organizations
    '''
    distinct_tracked_orgs = [x.user for x in TrackedOrganization.objects.order_by("user").distinct("user")]
    distinct_tracked_industry_geos = [x.user for x in TrackedIndustryGeo.objects.order_by("user").distinct("user")]
    return set(list(distinct_tracked_orgs) + list(distinct_tracked_industry_geos))
