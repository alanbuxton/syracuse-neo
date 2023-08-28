import re
from topics.models import Resource
from typing import List, Dict, Tuple, Set
import html

NODE_COLOR_SHAPES = {"Organization": ("#c7deff","box"),
            "Activity": ("#fade9d","diamond"),
            "Person": ("#fdcfff","ellipse"),
            }

EDGE_COLORS = { "spender": "red",
                "buyer": "red",
                "investor": "red",
                "receiver": "cyan",
                "vendor": "cyan",
                "participant": "orange",
                "protagonist": "yellow",
                "target": "green"}


def get_nodes_edges(source_node_id,relationships) -> Tuple[ List[Dict], List[Dict], Dict, Set[Resource] ]:
    node_data = [] # list of dicts - including basic serialzied info (includes URI)
    edge_data = [] # list of dicts
    serialized_nodes = {} # full details for each node, keyed by URI
    raw_nodes = set()
    for rel_type, direction, rel_node in relationships:
        val = rel_node.serialize()
        val["id"] = val["uri"]
        (color, shape) = NODE_COLOR_SHAPES.get(val["entityType"],("#defa9d","triangleDown"))
        node_vals = {**val, **{"color": color, "shape": shape}}
        if direction == "to":
            from_node_id = source_node_id
            to_node_id = rel_node.uri
            arrow_direction = "to"
        elif direction == "from":
            from_node_id = rel_node.uri
            to_node_id = source_node_id
            arrow_direction = "to"
        else:
            sorted_nodes = sorted([source_node_id,rel_node.uri],key=lambda x: x.uri)
            from_node_id = sorted_nodes[0]
            to_node_id = sorted_nodes[1]
            arrow_direction = "none"
        edge_color = EDGE_COLORS.get(rel_type)
        edge_vals = {"from": from_node_id, "to": to_node_id, "label": rel_type_to_edge_label(rel_type), "arrows": arrow_direction, "color": edge_color}
        node_data.append(node_vals)
        edge_data.append(edge_vals)
        serialized_nodes[html.escape(rel_node.uri)]=rel_node.serialize_no_none()
        raw_nodes.add(rel_node)
    return node_data, edge_data, serialized_nodes, raw_nodes

def source_uber_node(source_node, limit=100) -> Tuple[Dict,List[Resource]] | None:
    nodes = source_node.same_as()
    if len(nodes) > limit:
        return None
    all_nodes = nodes + [source_node]
    uber_node = {"clusteredURIs":set(),"names":set(),"basedInHighGeoNames":set(),
                    "basedInHighGeoNamesRDFURL":set(),"basedInHighGeoNamesURL":set(),
                    "descriptions":set(), "industries":set()}
    for node in all_nodes:
        uber_node["clusteredURIs"].add(node.uri)
        uber_node["names"].add(node.name)
        if node.basedInHighGeoName:
            uber_node["basedInHighGeoNames"].add(node.basedInHighGeoName)
        if node.basedInHighGeoNameRDF:
            uber_node["basedInHighGeoNamesRDFURL"].add(node.basedInHighGeoNameRDFURL)
            uber_node["basedInHighGeoNamesURL"].add(node.basedInHighGeoNameURL)
        if node.industry:
            uber_node["industries"].add(node.industry)
        if node.description:
            uber_node["descriptions"].add(node.description)
    js_friendly = {k:list(v) for k,v in uber_node.items() if len(v) > 0}
    return js_friendly, all_nodes



def graph_source_activity_target(source_node):
    idx = 0
    all_nodes = []
    all_edges = []
    node_details = {}

    root_node_data  = source_uber_node(source_node)
    if root_node_data is None:
        return None

    uber_node_data, root_nodes = root_node_data

    root_uri = source_node.uri
    uber_node_dict_tmp = {"id":root_uri,"label":source_node.name,"entityType":"Cluster","uri":source_node.uri}
    uber_node_dict = {**uber_node_data,**uber_node_dict_tmp}
    (color, shape) = NODE_COLOR_SHAPES.get(source_node.serialize()["entityType"],("#c7deff","box"))
    uber_node_vals = {**{"color":color,"shape":shape},**uber_node_dict}

    all_activity_nodes = set()
    for root_node in root_nodes:
        rels = root_node.all_directional_relationships()
        new_nodes, new_edges, new_node_data, activity_nodes = get_nodes_edges(root_uri,rels)
        for new_node in new_nodes:
            if new_node not in all_nodes:
                all_nodes.append(new_node)
        for new_edge in new_edges:
            if new_edge not in all_edges:
                all_edges.append(new_edge)
        node_details = {**node_details,**new_node_data}
        all_activity_nodes.update(activity_nodes)

    all_outer_org_nodes = set()
    for node in all_activity_nodes:
        rels = node.all_directional_relationships()
        new_nodes, new_edges, new_node_data, outer_org_nodes = get_nodes_edges(node.uri,rels)
        for new_node in new_nodes:
            if new_node not in all_nodes:
                if new_node["id"] not in uber_node_data["clusteredURIs"]:
                    all_nodes.append(new_node)
        for new_edge in new_edges:
            if new_edge["to"] in uber_node_data["clusteredURIs"]:
                new_edge["to"] = 'root'
            if new_edge["from"] in uber_node_data["clusteredURIs"]:
                new_edge["from"] = 'root'
            if new_edge not in all_edges:
                all_edges.append(new_edge)
        node_details = {**node_details,**new_node_data}
        all_outer_org_nodes.update(outer_org_nodes)

    outer_same_as = Resource.find_same_as_relationships(all_outer_org_nodes)
    for rel_type, node1, node2 in outer_same_as:
        edge_color = EDGE_COLORS.get(rel_type)
        edge_vals = {"from": node1.uri, "to": node2.uri, "label": rel_type_to_edge_label(rel_type), "arrows": "none", "color": edge_color}
        all_edges.append(edge_vals)

    all_nodes.append(uber_node_vals)
    node_details[root_uri] = uber_node_dict

    return all_nodes, all_edges, node_details


def rel_type_to_edge_label(text):
    text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).upper()
    text = re.sub("_GEO_NAME_","_GEONAME_",text)
    text = re.sub("R_D_F","RDF",text)
    return text
