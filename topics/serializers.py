from rest_framework import serializers
from collections import defaultdict
from .graph_utils import graph_centered_on
from .converters import CustomSerializer
from .timeline_utils import get_timeline_data
from .geo_utils import geo_select_list
from .models import IndustryCluster, Article
from .model_queries import org_family_tree
from .constants import BEGINNING_OF_TIME, ALL_TIME_MAGIC_NUMBER
from typing import Union, List
from datetime import date, timedelta
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
        related = instance.all_directional_relationships(source_names=Article.all_sources())
        related_by_group = defaultdict(list)
        for entry in related:
            other_node = entry["other_node"].serialize_no_none()
            other_node["doc_extract"] = entry.get("document_extract")
            related_by_group[entry["label"]].append(other_node)
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
                logger.debug(f"Already seen {edge_id}, ignoring")
                continue
            edge_data[edge_id] = self.generate_edge_data(activity,art,label,docExtract)
            edge_label = f"{label.title()} ({art.sourceOrganization} {art.datePublished.strftime('%b %Y')})"
            edges.append ( { "id": edge_id,
                        "from": f"{ parent_uri }", "to": f"{ node.uri }", "color": "blue", "label": edge_label, "arrows": "to" })
            if node.uri in node_data:
                logger.debug(f"Already seen {node.uri}, ignoring")
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
        source_names = source_names_from_str(self.context.get("source_str")) 
        earliest_doc_date = date_from_str_with_default(self.context.get("earliest_str"))
        parents, parents_children, org_children = org_family_tree(organization_uri, 
                                                                  combine_same_as_name_only=combine_same_as_name_only,
                                                                  relationships=clean_rels,
                                                                  source_names=source_names,
                                                                  earliest_doc_date=earliest_doc_date)
        nodes = []
        edges = []
        node_data = {}
        edge_data = {}
        
        for parent,_,_,_,_,_ in sorted(parents, key=lambda x: x[0].best_name):
            if parent.uri in node_data:
                logger.debug(f"Already seen {parent.uri}, ignoring")
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
            "document_sources": create_source_pretty_print_data(self.context.get("source_str")),
            "earliest_doc_date": create_earliest_date_pretty_print_data(self.context.get("earliest_str")),
        }

def create_source_pretty_print_data(text):
    sorted_core_sources = ", ".join(sorted(Article.core_sources()))
    core_entries = f"Core Sources ({sorted_core_sources})"
    if text is None:
        return core_entries
    result_entries = []
    vals = sorted(text.split(","))
    has_core = False
    has_all = False
    available_names = Article.available_source_names_dict()
    for x in vals:
        if x.lower() == "_all":
            sorted_all_sources = ", ".join(sorted(Article.all_sources()))
            result_entries.append(f"All Sources ({sorted_all_sources})")
            has_all = True
        elif x.lower() == "_core":
            result_entries.append(core_entries)
            has_core = True
        else:
            name = available_names.get(x.lower())
            if name:
                result_entries.append(name)
    return {
        "pretty_print_text": ", ".join(result_entries),
        "has_core": has_core,
        "has_all": has_all,
    }

def create_earliest_date_pretty_print_data(text,today=date.today()):
    min_date = date_from_str_with_default(text)
    one_year_ago = today - timedelta(days=365)
    if min_date == one_year_ago: one_year_ago = None
    three_years_ago = today - timedelta(days = (365*3))
    if min_date == three_years_ago: three_years_ago = None
    five_years_ago = today - timedelta(days = (365*4))
    if min_date == five_years_ago: five_years_ago = None

    return {"min_date": min_date,
            "one_year_ago": one_year_ago,
            "one_year_ago_fmt": one_year_ago.strftime("%Y-%m-%d") if one_year_ago else None,
            "three_years_ago": three_years_ago,
            "three_years_ago_fmt": three_years_ago.strftime("%Y-%m-%d") if three_years_ago else None,
            "five_years_ago": five_years_ago,
            "five_years_ago_fmt": five_years_ago.strftime("%Y-%m-%d") if five_years_ago else None,
            "all_time_flag": True if min_date == BEGINNING_OF_TIME else False,
            }

def date_from_str(text):
    if text == str(ALL_TIME_MAGIC_NUMBER):
        return BEGINNING_OF_TIME
    # parse yyyy-mm-dd format
    try:
        d = date.fromisoformat(text)
    except:
        d = None
    return d

def date_from_str_with_default(text, default_years_ago=5):
    d = date_from_str(text)
    if d is None:
        d = date.today() - timedelta(days = (365 * default_years_ago))
    return d

def source_names_from_str(text) -> Union[List,str]:
    '''
        Returns list of source names - which can include special
        terms "_all" for all sources or "_core" for core sources only
    '''
    if text is None:
        return Article.core_sources()
    vals = text.split(",")
    available_names = Article.available_source_names_dict()
    matches = []
    no_matches = []
    for x in vals:
        if x.lower() == "_all":
            return Article.all_sources()
        if x.lower() == "_core":
            matches.extend(Article.core_sources())
            continue
        name = available_names.get(x.lower())
        if name:
            matches.append(name)
        else:
            no_matches.append(x)
    if len(no_matches) > 0:
        logger.warning(f"Don't have any info from the following sources: {no_matches}")
    return matches

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
        source_names = source_names_from_str(self.context.get("source_str"))
        earliest_doc_date = date_from_str_with_default(self.context.get("earliest_str"))
        graph_data = graph_centered_on(instance,source_names=source_names,
                                       min_date=earliest_doc_date,
                                       **self.context)
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
        data["document_sources"] = create_source_pretty_print_data(self.context.get("source_str"))
        data["earliest_doc_date"] = create_earliest_date_pretty_print_data(self.context.get("earliest_str"))
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
        source_names = source_names_from_str(self.context.get("source_str"))
        earliest_doc_date = date_from_str_with_default(self.context.get("earliest_str"))
        groups, items, item_display_details, org_display_details = get_timeline_data(instance, combine_same_as_name_only, 
                                                                                     source_names, earliest_doc_date)
        resp = {"groups": groups, "items":items,
            "item_display_details":CustomSerializer(item_display_details),
            "org_name": instance.longest_name,
            "org_node": instance.uri,
            "org_display_details": CustomSerializer(org_display_details),
            "document_sources": create_source_pretty_print_data(self.context.get("source_str")),
            "earliest_doc_date": create_earliest_date_pretty_print_data(self.context.get("earliest_str")),
            }
        return resp
