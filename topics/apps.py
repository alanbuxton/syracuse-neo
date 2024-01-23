from django.apps import AppConfig

class TopicsConfig(AppConfig):
    name = 'topics'

    # def ready(self):
    #     from .geo_utils import load_geo_data
    #     load_geo_data()
