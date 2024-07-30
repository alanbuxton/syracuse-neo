from rest_framework import serializers
from collections import defaultdict
from .graph_utils import graph_centered_on
from .converters import CustomSerializer
from .timeline_utils import get_timeline_data
from .geo_utils import geo_select_list
from .models import IndustryCluster
from .model_queries import org_family_tree
import logging
logger = logging.getLogger(__name__)

class OrganizationSerializer(serializers.ModelSerializer):

    def to_representation(self,instance):
        repres = instance.serialize()
        splitted_uri = instance.split_uri()
        repres["splitted_uri"] = splitted_uri
        return repres

class ResourceSerializer(serializers.Serializer):
    def to_representation(self, instance):
        related = instance.all_directional_relationships()
        related_by_group = defaultdict(list)
        for entry in related:
            related_by_group[entry["label"]].append(entry["other_node"])
        return {
            "resource": instance.serialize_no_none(),
            "relationships": dict(related_by_group),
        }

class FamilyTreeSerializer(serializers.BaseSerializer):

    def generate_edge_data(self, activity, article, label, docExtract):
        return {
            "activity_type": label,
            "source_organization": article.sourceOrganization,
            "date_published": article.datePublished,
            "headline": article.headline,
            "document_extract": docExtract,
            "activity_status": activity.status_as_string,
            "document_url": article.documentURL,
            "internal_archive_page_url": article.archiveOrgPageURL,
            "internet_archive_list_url": article.archiveOrgListURL,
            "activity_uri": activity.uri,
            "article_uri": article.uri,
        }

    def add_child_nodes_edges(self, central_uri, nodes, node_data, edges, edge_data, parent_uri, family_tree_results, level):
        for _,node,activity,art,label,docExtract in sorted(family_tree_results , key=lambda x: x[0].best_name):
            edge_id = f"{ parent_uri }-{ label }-{ node.uri }"
            if edge_id in edge_data:
                logger.info(f"Already seen {edge_id}, ignoring")
                continue
            edge_data[edge_id] = self.generate_edge_data(activity,art,label,docExtract)
            edge_label = f"{label.title()} ({art.sourceOrganization} {art.datePublished.strftime('%b %Y')})"
            edges.append ( { "id": edge_id,
                        "from": f"{ parent_uri }", "to": f"{ node.uri }", "color": "blue", "label": edge_label, "arrows": "to" })
            if node.uri in node_data:
                logger.info(f"Already seen {node.uri}, ignoring")
                continue
            node_data[node.uri] = node.serialize_no_none()
            node_attribs = {"id": node.uri, "label": node.best_name, "level": level}
            if node.uri == central_uri:
                node_attribs['color'] = '#f6c655'
            nodes.append(node_attribs)


    def to_representation(self, instance):
        organization_uri = instance.uri
        combine_same_as_name_only = self.context.get('combine_same_as_name_only',True)
        rels = self.context.get('relationship_str','buyer,vendor')
        rels = rels.replace(",","|")
        clean_rels = only_valid_relationships(rels)
        parents, parents_children, org_children = org_family_tree(organization_uri, 
                                                                  combine_same_as_name_only=combine_same_as_name_only,
                                                                  relationships=clean_rels)
        nodes = []
        edges = []
        node_data = {}
        edge_data = {}
        
        for parent,_,_,_,_,_ in sorted(parents, key=lambda x: x[0].best_name):
            if parent.uri in node_data:
                logger.info(f"Already seen {parent.uri}, ignoring")
                continue
            node_data[parent.uri] = parent.serialize_no_none()
            nodes.append( {"id": parent.uri, "label": parent.best_name, "level": 0})
            relevant_children = [x for x in parents_children if x[0] == parent]
            self.add_child_nodes_edges(organization_uri, nodes, node_data, edges, edge_data, parent.uri, relevant_children, 1)

        self.add_child_nodes_edges(organization_uri, nodes, node_data, edges, edge_data, organization_uri, org_children, 2)

        if organization_uri not in node_data:
            node_data[organization_uri] = instance.serialize_no_none()
            node_attribs = {"id": organization_uri, "label": instance.best_name, "level": 1}
            node_attribs['color'] = '#f6c655'
            nodes.append(node_attribs)

        pruned_nodes = prune_not_needed_nodes(nodes, edges) 
        pruned_node_data = {}
        for node in pruned_nodes:
            pruned_node_data[node['id']] = node_data[node['id']]

        sorted_edges = sort_edges(edges, pruned_nodes)

        return {
            "nodes": pruned_nodes,
            "node_details": CustomSerializer(pruned_node_data),
            "edges": sorted_edges,
            "edge_details": CustomSerializer(edge_data),
        }


def only_valid_relationships(text):
    full_text = text
    valid_rels = ["buyer","vendor","investor"]
    for rel in valid_rels:
        text = text.replace(rel,"")
    text = text.replace("|","")
    if len(text) != 0:
        logger.warning(f"Got illegal relationships {text}")
        full_text = "buyer|vendor"
    return full_text

def sort_edges(edges, nodes):
    edge_dict_to = defaultdict(list)
    for x in edges:
        edge_dict_to[x['to']].append(x)
    sorted_edges = []
    for node in nodes:
        edges = edge_dict_to.pop(node['id'],[]) 
        sorted_edges.extend(edges)
    for remaining_edges in edge_dict_to.values():
        sorted_edges.extend(remaining_edges)
    return sorted_edges

def prune_not_needed_nodes(nodes, edges):
    nodes_to_keep = []
    for node in nodes:
        for edge in edges:
            if node['id'] == edge['from'] or node['id'] == edge['to']:
                nodes_to_keep.append(node)
                break
    nodes_to_keep = sorted(nodes_to_keep, key=lambda x: f"{x['level']}-{x['label']}")
    return nodes_to_keep



class OrganizationGraphSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        graph_data = graph_centered_on(instance,**self.context)
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
        nodes_by_type = {}
        for node_row in clean_node_data:
            node_type = node_row['entity_type']
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node_row['id'])
        data["nodes_by_type"] = nodes_by_type
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
    industry =  DataListChoiceField(choices=IndustryCluster.representative_docs_to_industry())

    def get_industry_id(self):
        self.is_valid()
        return self['industry'].value

class GeoSerializer(serializers.Serializer):
    country_or_region = DataListChoiceField(choices=geo_select_list(include_alt_names=True))

    def get_country_or_region_id(self):
        self.is_valid()
        return self['country_or_region'].value 

class OrganizationTimelineSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        combine_same_as_name_only = self.context.get("combine_same_as_name_only",True)
        groups, items, item_display_details, org_display_details = get_timeline_data(instance, combine_same_as_name_only)
        resp = {"groups": groups, "items":items,
            "item_display_details":CustomSerializer(item_display_details),
            "org_name": instance.longest_name,
            "org_node": instance.uri,
            "org_display_details": CustomSerializer(org_display_details),
            }
        return resp
