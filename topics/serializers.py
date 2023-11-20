from rest_framework import serializers
from .graph_utils import graph_source_activity_target
from .converters import CustomSerializer
from .timeline_utils import get_timeline_data

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
                "source_node_name": instance.name,
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

class IndustrySearchSerializer(serializers.Serializer):
    industry_name = serializers.CharField(
        max_length=20,
        style={'placeholder': 'Search ...', 'autofocus': True}
    )


class GeoSerializer(serializers.Serializer):
    selected_country = serializers.ChoiceField(choices=[])

    def __init__(self, *args, **kwargs):
        orig_choices = kwargs.pop('choices',[])
        if "initial" in kwargs:
            selected_country = kwargs.pop("initial")
        else:
            selected_country = None
        choices = sorted([(v,k) for k,v in orig_choices.items()], key=lambda x: x[1])
        super().__init__(*args, **kwargs)
        self.fields['selected_country'].choices = choices
        if selected_country is not None:
            self.fields['selected_country'].initial = selected_country

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
            "org_name": instance.name,
            "org_node": instance.uri,
            "org_display_details": CustomSerializer(org_display_details),
            "errors": errors,
            }
        return resp


class DateRangeSerializer(serializers.Serializer):
    from_date = serializers.DateField()
    to_date = serializers.DateField()
