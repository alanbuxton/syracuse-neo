from rest_framework import serializers
from .graph_utils import graph_to_depth

class OrganizationSerializer(serializers.BaseSerializer):

    def to_representation(self,instance):
        repres = instance.serialize()
        splitted_uri = instance.split_uri()
        repres["splitted_uri"] = splitted_uri
        return repres


class OrganizationGraphSerializer(serializers.BaseSerializer):

    def to_representation(self, instance):
        node_data, edge_data, node_details = graph_to_depth(source_node=instance,max_depth=4)
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
        data = {}
        data["node_data"] = clean_node_data
        data["edge_data"] = clean_edge_data
        data["source_node"] = f"{instance.name} ({instance.uri})"
        data["node_details"] = node_details
        return data


class SearchSerializer(serializers.Serializer):
    search_for = serializers.CharField(
        max_length=50,
        style={'placeholder': 'Search ...', 'autofocus': True}
    )
