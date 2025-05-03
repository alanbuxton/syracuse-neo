# fields.py
from django.urls import reverse
from rest_framework.relations import HyperlinkedRelatedField
import logging 

logger = logging.getLogger(__name__)

class HyperlinkedRelationshipField(HyperlinkedRelatedField):
    def __init__(self, *args, many=False, **kwargs):
        self._many = many  # store manually
        kwargs['read_only'] = True  # prevents DRF from requiring queryset
        super().__init__(*args, **kwargs)

    def to_representation(self, value):
        if self._many:
            return [
                self.get_url(item, self.view_name, self.context["request"], self.format)
                for item in value.all()
            ]
        return self.get_url(value, self.view_name, self.context["request"], self.format)

    def get_url(self, obj, view_name, request, format):
        kwargs = {"pk": obj.pk}
        return self.reverse(view_name, kwargs=kwargs, request=request, format=format)