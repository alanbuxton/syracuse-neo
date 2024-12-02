from django.db import models
from datetime import datetime, timezone
from django.core.serializers.json import DjangoJSONEncoder
from django.core.cache import cache

LAST_UPDATED_KEY="last_updated"
GEO_DATA_KEY="geo_data"
STATS_PREFIX="stats"
# INDUSTRY_CLUSTER_GEO_COUNTS_KEY="industry_cluster_geo_counts"
INDUSTRY_CLUSTER_GEO_ORGS_KEY="industry_cluster_geo_orgs"

def cat_key(root, extra): # to avoid typos
    return f"{root}-{extra}"
    
class CustomEncoder(DjangoJSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        return super().default(o)

# https://stackoverflow.com/a/20674112
def get_or_none(**kwargs):
    try:
        return PrecalculatedData.objects.get(**kwargs)
    except PrecalculatedData.DoesNotExist:
        return None

def key_exists(key):
    cnt = PrecalculatedData.objects.filter(key=key).count()
    if cnt > 0:
        return True
    else:
        return False

class PrecalculatedData(models.Model):
    key = models.TextField(db_index=True,unique=True)
    json_value = models.JSONField(null=True,encoder=CustomEncoder)
    datetime_value = models.DateTimeField(null=True)

    @staticmethod
    def get_value(key, field):
        obj = get_or_none(key=key) 
        if obj is None:
            return None
        return getattr(obj,field)

    @staticmethod
    def set_value(key, field, value):
        obj = get_or_none(key=key)
        if obj is None:
            obj = PrecalculatedData(key=key)
        setattr(obj,field,value)
        obj.save()
        return getattr(obj, field)

    @staticmethod
    def get_last_updated():
        return P.get_value(LAST_UPDATED_KEY,"datetime_value")

    @staticmethod
    def set_last_updated(ts=datetime.now(tz=timezone.utc)):
        return P.set_value(LAST_UPDATED_KEY,"datetime_value",ts)

    @staticmethod
    def get_geo_data():
        return P.get_value(GEO_DATA_KEY,"json_value")
    
    @staticmethod
    def has_geo_data():
        return key_exists(GEO_DATA_KEY)

    @staticmethod
    def set_geo_data(data):
        return P.set_value(GEO_DATA_KEY,"json_value",data)
    
    @staticmethod
    def has_industry_cluster_geo_orgs(topic_id):
        return key_exists(cat_key(INDUSTRY_CLUSTER_GEO_ORGS_KEY,topic_id))
    
    # @staticmethod
    # def set_industry_cluster_geo_counts(topic_id,data):
    #     return P.set_value(cat_key(INDUSTRY_CLUSTER_GEO_COUNTS_KEY,topic_id),"json_value",data)
    
    # @staticmethod
    # def get_industry_cluster_geo_counts(topic_id):
    #     return P.get_value(cat_key(INDUSTRY_CLUSTER_GEO_COUNTS_KEY,topic_id),"json_value")
    
    @staticmethod
    def set_industry_cluster_geo_orgs(topic_id, data):
        return P.set_value(cat_key(INDUSTRY_CLUSTER_GEO_ORGS_KEY,topic_id),"json_value",data) 
    
    @staticmethod
    def get_industry_cluster_geo_orgs(topic_id):
        return P.get_value(cat_key(INDUSTRY_CLUSTER_GEO_ORGS_KEY,topic_id),"json_value")


    @staticmethod
    def get_stats(date):
        key = f"{STATS_PREFIX}_{date}"
        return P.get_value(key,"json_value")

    @staticmethod
    def set_stats(date, new_data):
        key = f"{STATS_PREFIX}_{date}"
        return P.set_value(key, "json_value",new_data)

    @staticmethod
    def nuke_all(including_cache=True):
        P.objects.all().delete()
        if including_cache is True:
            cache.clear()

P = PrecalculatedData # save your keyboard

def cache_last_updated_date():
    if P.get_last_updated() is None:
        return False
    else:
        return P.get_last_updated()
