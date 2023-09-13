from django.apps import AppConfig
from .geo_utils import load_geo_data


class TopicsConfig(AppConfig):
    name = 'topics'

    def ready(self):
        load_geo_data()
