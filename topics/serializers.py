from rest_framework import serializers
from .graph_utils import graph_source_activity_target
from .converters import CustomSerializer

class OrganizationSerializer(serializers.BaseSerializer):

    def to_representation(self,instance):
        repres = instance.serialize()
        splitted_uri = instance.split_uri()
        repres["splitted_uri"] = splitted_uri
        return repres


class OrganizationGraphSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        graph_data = graph_source_activity_target(source_node=instance,**self.context)
        data = {"source_node": f"{instance.name} ({instance.uri})",
                "too_many_nodes":False}
        if graph_data is None:
            data["node_data"] = []
            data["edge_data"] = []
            data["too_many_nodes"] = True
            return data
        node_data, edge_data, node_details, edge_details = graph_data
        seen_nodes = [] # list of ids
        seen_edges = [] # tuple of from, to, type
        clean_node_data = []
        clean_edge_data = []
        for node in node_data:
            if node["id"] in seen_nodes:
                continue
            seen_nodes.append(node["id"])
            clean_node_data.append(node)
        for edge in edge_data:
            tup = (edge["from"],edge["to"],edge["label"],edge["arrows"])
            if tup in seen_edges:
                continue
            seen_edges.append(tup)
            clean_edge_data.append(edge)
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

class GeoSerializer(serializers.Serializer):
    selected_country = serializers.ChoiceField(choices=[])

    def __init__(self, *args, **kwargs):
        orig_choices = kwargs.pop('choices',[])
        choices = sorted([(v,k) for k,v in orig_choices.items()], key=lambda x: x[1])
        super().__init__(*args, **kwargs)
        self.fields['selected_country'].choices = choices
        self.fields['selected_country'].default = 'United States'
        self.fields['selected_country'].initial = 'United States'


class DateRangeSerializer(serializers.Serializer):
    from_date = serializers.DateField()
    to_date = serializers.DateField()
