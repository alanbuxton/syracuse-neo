from rest_framework import serializers
from .graph_utils import graph_source_activity_target
from .converters import CustomSerializer
from .timeline_utils import get_timeline_data
from .geo_utils import geo_select_list
from .industry_utils import industry_select_list

class OrganizationSerializer(serializers.BaseSerializer):

    def to_representation(self,instance):
        repres = instance.serialize()
        splitted_uri = instance.split_uri()
        repres["splitted_uri"] = splitted_uri
        return repres


class OrganizationGraphSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        graph_data = graph_source_activity_target(source_node=instance,**self.context)
        data = {"source_node": instance.uri,
                "source_node_name": instance.longest_name,
                "too_many_nodes":False}
        if graph_data is None:
            data["node_data"] = []
            data["edge_data"] = []
            data["too_many_nodes"] = True
            return data
        clean_node_data, clean_edge_data, node_details, edge_details = graph_data
        data["node_data"] = clean_node_data
        data["edge_data"] = clean_edge_data
        data["node_details"] = CustomSerializer(node_details)
        data["edge_details"] = CustomSerializer(edge_details)
        return data

class NameSearchSerializer(serializers.Serializer):
    name = serializers.CharField(
        max_length=20,
        style={'placeholder': 'Search ...', 'autofocus': True}
    )

class DataListChoiceField(serializers.ChoiceField):

    def __init__(self, choices, **kwargs):
        super().__init__(choices, **kwargs)
        self.choices = [x for _,x in choices]
        self.style = {"base_template": "datalist-select.html"}
        self.text_to_id = {v:k for k,v in choices}

    def to_internal_value(self, data):
        ''' data list allows for any text to be entered,
            so provided text won't always match '''
        res = self.text_to_id.get(data,None)
        return res

class IndustrySerializer(serializers.Serializer):
    industry =  DataListChoiceField(choices=industry_select_list())

    def get_industry_id(self):
        self.is_valid()
        return self['industry'].value

class GeoSerializer(serializers.Serializer):
    country_or_region = DataListChoiceField(choices=geo_select_list(include_alt_names=True))

    def get_country_or_region_id(self):
        self.is_valid()
        return self['country_or_region'].value

class TimelineSerializer(serializers.Serializer):
    def to_representation(self, instance, **kwargs):
        limit = 10
        groups, items, item_display_details, org_display_details, errors = get_timeline_data(instance, limit)
        errors = sorted(errors)
        if len(errors) > 50:
            errors = "; ".join(errors[:50]) + f" plus {len(errors) - 50} other organizations"
        else:
            errors = "; ".join(errors)
        if len(groups) + len(errors) > limit:
            limit_message = f'Max {limit} organizations shown for web users. Please <a href="mailto:info-syracuse@1145.am?subject=Want%20to%20see%20more%20Syracuse%20data&body=Dear%20Info%0D%0AI%20would%20like%20to%20discuss%20accessing%20timeline%20data.">contact us</a> for API or bulk data.'
        else:
            limit_message = ""
        resp = {"groups": groups, "items":items,
            "item_display_details":CustomSerializer(item_display_details),
            "org_display_details": CustomSerializer(org_display_details),
            "errors": errors,
            "limit_msg": limit_message,
            }
        return resp

class OrganizationTimelineSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([instance], None)
        errors = sorted(errors)
        if len(errors) > 50:
            errors = "; ".join(errors[:50]) + f" plus {len(errors) - 50} other organizations"
        else:
            errors = "; ".join(errors)
        resp = {"groups": groups, "items":items,
            "item_display_details":CustomSerializer(item_display_details),
            "org_name": instance.longest_name,
            "org_node": instance.uri,
            "org_display_details": CustomSerializer(org_display_details),
            "errors": errors,
            }
        return resp
