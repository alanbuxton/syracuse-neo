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
        node_data, edge_data, node_details = graph_data
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
        return data


class SearchSerializer(serializers.Serializer):
    search_for = serializers.CharField(
        max_length=20,
        style={'placeholder': 'Search ...', 'autofocus': True}
    )

class DateRangeSerializer(serializers.Serializer):
    from_date = serializers.DateField()
    to_date = serializers.DateField()
