import re
from topics.models import Organization, Person, ActivityMixin, Resource, Role
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
    serialized_nodes = {} # full details for each node, keyed by URI, for detailed node display
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
        edge_display_data = {"from":from_node_id,"to":to_node_id,"relationship":rel_type,"documentURL":node_vals["documentURL"],"documentTitle":node_vals["documentTitle"]}
        if node_vals.get("documentExtract"):
            edge_display_data["documentExtract"] = node_vals["documentExtract"]
        serialized_edges[edge_vals["id"]]=edge_display_data
        raw_nodes.add(rel_node)
    return node_data, edge_data, serialized_nodes, raw_nodes, serialized_edges

def get_node_cluster(source_node, limit=100) -> Tuple[Dict,List[Resource],Dict,List] | Set:
    nodes = source_node.same_as()
    if len(nodes) > limit:
        logger.warning(f"Found {len(nodes)} nodes for {source_node.uri} (limit = {limit})- not continuing")
        errors = { "error_names": set( [ x.name for x in nodes] ), "error_orgs": nodes }
        return errors
    all_nodes = nodes + [source_node]
    uri_mapping = {}
    node_cluster = {"clusteredURIs":set(),"names":set(),"basedInHighGeoNames":set(),
                    "basedInHighGeoNamesRDFURLs":set(),"basedInHighGeoNamesURLs":set(),
                    "descriptions":set(), "industries":set()}
    for node in all_nodes:
        if node.uri != source_node.uri:
            uri_mapping[node.uri] = source_node.uri
        node_cluster["clusteredURIs"].add(node.uri)
        node_cluster["names"].add(node.name)
        if node.basedInHighGeoName:
            node_cluster["basedInHighGeoNames"].add(node.basedInHighGeoName)
        if node.basedInHighGeoNameRDF:
            node_cluster["basedInHighGeoNamesRDFURLs"].add(node.basedInHighGeoNameRDFURL)
            node_cluster["basedInHighGeoNamesURLs"].add(node.basedInHighGeoNameURL)
        if node.industry:
            node_cluster["industries"].add(node.industry)
        if node.description:
            node_cluster["descriptions"].add(node.description)
    data_for_display = {k:"; ".join(v) for k,v in node_cluster.items() if len(v) > 0}
    return data_for_display, all_nodes, uri_mapping


def graph_source_activity_target(source_node, **kwargs):
    root_node_data = get_node_cluster(source_node)
    if isinstance(root_node_data, dict) and root_node_data.get("error_names") is not None:
        return None
    root_node_cluster = get_node_cluster(source_node)
    root_node_display_data, root_nodes, uri_mapping = root_node_cluster
    root_uri = source_node.uri
    root_node_dict_tmp = {"label":source_node.name,"entityType":"Cluster","uri":root_uri}
    root_node_dict = {**root_node_display_data,**root_node_dict_tmp}
    (color, shape) = node_color_shape(source_node)
    root_node_data = {**{"id":root_uri,"color":color,"shape":shape},**root_node_dict}
    all_nodes = [root_node_data]
    node_details= {root_uri: root_node_dict}
    future_round_raw_nodes = root_nodes.copy()
    seen_raw_nodes = future_round_raw_nodes.copy()
    all_edges = []
    edge_details = {}

    while len(future_round_raw_nodes) > 0:
        future_round_raw_nodes, seen_raw_nodes, all_nodes, node_details, all_edges, edge_details = add_next_round_of_graph(
            future_round_raw_nodes, seen_raw_nodes, all_nodes, all_edges, node_details, edge_details, uri_mapping)
        future_round_raw_nodes = [x for x in future_round_raw_nodes if
                        isinstance(x,ActivityMixin) or isinstance(x,Role)]

    for node in seen_raw_nodes:
        for a,b in zip ( ["basedInHighGeoName","nameGeoName","whereGeoName" ], ["basedIn","where","where"]):
            res = get_loc_node_if_exists(node, a,b, node_details)
            if res is None:
                continue
            node_js_data, node_display_data, edge_js_data, edge_display_data = res
            if node_js_data not in all_nodes:
                all_nodes.append(node_js_data)
                node_details[node_js_data['uri']] = node_display_data
            if edge_js_data not in all_edges:
                edge_js_data["to"] = uri_mapping.get(edge_js_data["to"],edge_js_data["to"])
                edge_js_data["from"] = uri_mapping.get(edge_js_data["from"],edge_js_data["from"])
                all_edges.append(edge_js_data)
                edge_details[edge_js_data["id"]] = edge_display_data

    same_as_rels = Resource.find_same_as_relationships(set(seen_raw_nodes) - set(root_nodes))
    for rel_type, node1, node2 in same_as_rels:
        edge_color = EDGE_COLORS.get(rel_type)
        edge_label = rel_type_to_edge_label(rel_type)
        edge_vals = {"id": f"{node1.uri}-{node2.uri}-{rel_type}","from": node1.uri, "to": node2.uri, "label": edge_label, "arrows": "none", "color": edge_color}
        if edge_vals not in all_edges:
            all_edges.append(edge_vals)
        edge_details[edge_vals["id"]] = {"from":node1.uri,"to":node2.uri,"relationship":rel_type}

    cleaned_nodes, cleaned_edges = clean_graph_data(all_nodes, all_edges)

    return cleaned_nodes, cleaned_edges, node_details, edge_details


def get_loc_node_if_exists(node, field, edge_name, node_details, location_node_color=LOCATION_NODE_COLOR):
    url_field = field + "URL"
    if not hasattr(node, url_field):
        return None
    loc_uri = getattr(node,url_field)
    if loc_uri is None:
        return None
    node_display_details = {"label":getattr(node, field), "entityType":"Location","uri":loc_uri}
    node_extra_js_data = {"id": loc_uri, "color": location_node_color}
    related_node = node_details[node.uri]
    edge_display_details = {"from":node.uri,"to":loc_uri,"relationship":edge_name}
    if related_node.get("documentTitle"):
        edge_display_details["documentTitle"]=related_node["documentTitle"]
    if related_node.get("documentURL"):
        edge_display_details["documentURL"]=related_node["documentURL"]
    if related_node.get("documentExtract"):
        edge_display_details["documentExtract"] = related_node["documentExtract"]
    edge_upper = rel_type_to_edge_label(edge_name)
    edge_extra_js_data = {"id": f"{node.uri}-{loc_uri}-{edge_upper}","label": edge_upper, "arrows": "to", "color": "black"}
    return {**node_display_details,**node_extra_js_data}, node_display_details, {**edge_display_details,**edge_extra_js_data}, edge_display_details


def add_next_round_of_graph(next_round_nodes, seen_raw_nodes, all_nodes, all_edges, node_details, edge_details, uri_mapping):
    future_round_raw_nodes = []
    for node in next_round_nodes:
        rels = node.all_directional_relationships()
        new_node_data, new_edge_data, serialized_nodes, raw_nodes, serialized_edges = get_nodes_edges(node.uri,rels)
        for new_node in new_node_data:
            if new_node not in all_nodes and new_node['uri'] not in uri_mapping.keys() :
                all_nodes.append(new_node)
        for new_edge in new_edge_data:
            new_edge["to"] = uri_mapping.get(new_edge["to"],new_edge["to"])
            new_edge["from"] = uri_mapping.get(new_edge["from"],new_edge["from"])
            if new_edge not in all_edges:
                all_edges.append(new_edge)
        for raw_node in raw_nodes:
            if raw_node not in seen_raw_nodes:
                future_round_raw_nodes.append(raw_node)
                seen_raw_nodes.append(raw_node)
        node_details = {**serialized_nodes,**node_details} # Don't overwrite existing node details with new ones
        edge_details = {**serialized_edges,**edge_details}
    return future_round_raw_nodes, seen_raw_nodes, all_nodes, node_details, all_edges, edge_details

def rel_type_to_edge_label(text):
    text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).upper()
    text = re.sub(r'^REL_','',text)
    text = re.sub("_GEO_NAME_","_GEONAME_",text)
    text = re.sub("R_D_F","RDF",text)
    return text

def clean_graph_data(node_data, edge_data):
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
    return clean_node_data, clean_edge_data
