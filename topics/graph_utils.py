import re
from topics.models import Organization, Person, ActivityMixin, Resource
from typing import List, Dict, Tuple, Set
import logging

logger = logging.getLogger("syracuse")

EDGE_COLORS = { "spender": "red",
                "buyer": "red",
                "investor": "red",
                "receiver": "cyan",
                "vendor": "cyan",
                "participant": "orange",
                "protagonist": "salmon",
                "target": "green"}

LOCATION_NODE_COLOR = "#cddb9a"

def node_color_shape(node):
    if isinstance(node, Organization):
        return ("#c7deff","box")
    elif isinstance(node, Person):
        return ("#f5e342","ellipse")
    elif issubclass(node.__class__, ActivityMixin):
        if not hasattr(node, "status"):
            return ("#f6c655","diamond")
        if node.when is not None:
            return ("#f6c655","diamond")
        elif node.status == "has not happened":
            return ("#ffe6ff","diamond")
        else:
            return ("#b3ffff","diamond")
    return ("#defa9d","triangleDown")

def get_nodes_edges(source_node_id,relationships) -> Tuple[ List[Dict], List[Dict], Dict, Set[Resource] ]:
    node_data = [] # list of dicts - including basic serialzied info (includes URI)
    edge_data = [] # list of dicts
    serialized_nodes = {} # full details for each node, keyed by URI
    raw_nodes = set()
    serialized_edges = {}
    for rel_type, direction, rel_node in relationships:
        val = rel_node.serialize()
        val["id"] = val["uri"]
        (color, shape) = node_color_shape(rel_node)
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
        edge_label = rel_type_to_edge_label(rel_type)
        edge_vals = {"id": f"{from_node_id}-{to_node_id}-{edge_label}", "from": from_node_id, "to": to_node_id, "label": edge_label, "arrows": arrow_direction, "color": edge_color}
        node_data.append(node_vals)
        edge_data.append(edge_vals)
        serialized_nodes[rel_node.uri]=rel_node.serialize_no_none()
        serialized_edges[edge_vals["id"]]={"from":from_node_id,"to":to_node_id,"relationship":rel_type}
        raw_nodes.add(rel_node)
    return node_data, edge_data, serialized_nodes, raw_nodes, serialized_edges

def source_uber_node(source_node, limit=100) -> Tuple[Dict,List[Resource]] | None:
    nodes = source_node.same_as()
    if len(nodes) > limit:
        logger.warning(f"Found {len(nodes)} nodes for {source_node.uri} (limit = {limit})- not continuing")
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
    js_friendly = {k:" ".join(v) for k,v in uber_node.items() if len(v) > 0}
    return js_friendly, all_nodes


def graph_source_activity_target(source_node, **kwargs):
    all_nodes = []
    all_edges = []
    node_details = {}
    edge_details = {}

    root_node_data  = source_uber_node(source_node)
    if root_node_data is None:
        return None

    uber_node_data, root_nodes = root_node_data

    root_uri = source_node.uri
    uber_node_dict_tmp = {"label":source_node.name,"entityType":"Cluster","uri":root_uri}
    uber_node_dict = {**uber_node_data,**uber_node_dict_tmp}
    (color, shape) = node_color_shape(source_node)
    uber_node_vals = {**{"id":root_uri,"color":color,"shape":shape},**uber_node_dict}

    l1_activity_nodes = set()
    for root_node in root_nodes:
        rels = root_node.all_directional_relationships(**kwargs)
        new_nodes, new_edges, new_node_data, activity_nodes, new_edge_data = get_nodes_edges(root_uri,rels)
        for new_node in new_nodes:
            if new_node not in all_nodes:
                all_nodes.append(new_node)
        for new_edge in new_edges:
            if new_edge not in all_edges:
                all_edges.append(new_edge)
        node_details = {**node_details,**new_node_data}
        edge_details = {**edge_details, **new_edge_data}
        l1_activity_nodes.update(activity_nodes)

    all_outer_nodes = set()
    new_nodes, all_nodes, node_details, all_edges, edge_details = add_next_round_of_graph(l1_activity_nodes, all_nodes, uber_node_data, all_edges, node_details, edge_details)
    all_outer_nodes.update(new_nodes)
    new_nodes, all_nodes, node_details, all_edges, edge_details = add_next_round_of_graph(new_nodes, all_nodes, uber_node_data, all_edges, node_details, edge_details)
    all_outer_nodes.update(new_nodes)

    outer_same_as = Resource.find_same_as_relationships(all_outer_nodes)
    for rel_type, node1, node2 in outer_same_as:
        edge_color = EDGE_COLORS.get(rel_type)
        edge_label = rel_type_to_edge_label(rel_type)
        edge_vals = {"id": f"{node1.uri}-{node2.uri}-{edge_label}","from": node1.uri, "to": node2.uri, "label": edge_label, "arrows": "none", "color": edge_color}
        if edge_vals not in all_edges:
            all_edges.append(edge_vals)
        edge_details[edge_vals["id"]] = {"from":node1.uri,"to":node2.uri,"relationship":rel_type}

    for node in set(root_nodes + list(all_outer_nodes)):
        if not hasattr(node, "basedInHighGeoNameURL"):
            continue
        loc_uri = node.basedInHighGeoNameURL
        if loc_uri is None:
            continue
        loc_node = {"id":loc_uri,"label":node.basedInHighGeoName,"entityType":"Location","uri":loc_uri,"color": LOCATION_NODE_COLOR}
        if loc_node not in all_nodes:
            all_nodes.append(loc_node)
            node_details[loc_uri]={"label":node.basedInHighGeoName,"entityType":"Location","uri":loc_uri}
        loc_edge = {"id": f"{node.uri}-{loc_uri}-BASED_IN","from": node.uri, "to": loc_uri, "label": "BASED_IN", "arrows": "to", "color": "black"}
        if loc_edge not in all_edges:
            all_edges.append(loc_edge)
        edge_details[loc_edge["id"]] = {"from":node.uri,"to":loc_uri,"relationship":"basedIn"}

    for node in all_outer_nodes:
        if not hasattr(node, "nameGeoNameURL"):
            continue
        loc_uri = node.nameGeoNameURL
        if loc_uri is None:
            continue
        loc_node = {"id":loc_uri,"label":node.nameGeoName,"entityType":"Location","uri":loc_uri,"color": LOCATION_NODE_COLOR}
        if loc_node not in all_nodes:
            all_nodes.append(loc_node)
            node_details[loc_uri]={"label":node.nameGeoName,"entityType":"Location","uri":loc_uri}
        loc_edge = {"id": f"{node.uri}-{loc_uri}-WHERE","from": node.uri, "to": loc_uri, "label": "WHERE", "arrows": "to", "color": "black"}
        if loc_edge not in all_edges:
            all_edges.append(loc_edge)
        edge_details[loc_edge["id"]] = {"from":node.uri,"to":loc_uri,"relationship":"where"}

    for node in l1_activity_nodes:
        if not hasattr(node, "whereGeoNameURL"):
            continue
        loc_uri = node.whereGeoNameURL
        if loc_uri is None:
            continue
        loc_node = {"id":loc_uri,"label":node.whereGeoName,"entityType":"Location","uri":loc_uri,"color": LOCATION_NODE_COLOR}
        if loc_node not in all_nodes:
            all_nodes.append(loc_node)
            node_details[loc_uri]={"label":node.whereGeoName,"entityType":"Location","uri":loc_uri}
        loc_edge = {"id": f"{node.uri}-{loc_uri}-WHERE","from": node.uri, "to": loc_uri, "label": "WHERE", "arrows": "to", "color": "black"}
        if loc_edge not in all_edges:
            all_edges.append(loc_edge)
        edge_details[loc_edge["id"]] = {"from":node.uri,"to":loc_uri,"relationship":"where"}

    all_nodes.append(uber_node_vals)
    node_details[root_uri] = uber_node_dict
    return all_nodes, all_edges, node_details, edge_details


def add_next_round_of_graph(all_activity_nodes, all_nodes, uber_node_data, all_edges, node_details, edge_details):
    all_outer_org_nodes = set()
    for node in all_activity_nodes:
        rels = node.all_directional_relationships()
        new_nodes, new_edges, new_node_data, outer_org_nodes, new_edge_data = get_nodes_edges(node.uri,rels)
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
        edge_details = {**edge_details, **new_edge_data}
        all_outer_org_nodes.update(outer_org_nodes)
    return all_outer_org_nodes, all_nodes, node_details, all_edges, edge_details

def rel_type_to_edge_label(text):
    text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).upper()
    text = re.sub("_GEO_NAME_","_GEONAME_",text)
    text = re.sub("R_D_F","RDF",text)
    return text
