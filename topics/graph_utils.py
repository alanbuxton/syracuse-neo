import re
from topics.models import Organization, Person, ActivityMixin, Resource, Role, Article, IndustryCluster
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
                "target": "green",
                "documentSource": "black"}

LOCATION_NODE_COLOR = "#cddb9a"

def node_color_shape(node):
    if isinstance(node, Organization):
        return ("#c7deff","box")
    elif isinstance(node, Person):
        return ("#f5e342","ellipse")
    elif isinstance(node, ActivityMixin):
        return ("#f5c4ff","hexagon")
    elif isinstance(node, Article):
        return ("#f6c655","diamond")
    elif isinstance(node, IndustryCluster):
        return ("#cf6e1f","diamond")
    return ("#defa9d","triangleDown")

def graph_centered_on(start_node, **kwargs):
    root_node = Resource.self_or_ultimate_target_node(start_node)
    root_uri = root_node.uri
    node_data = [ resource_to_node_data(root_node) ] # Nodes for graph
    node_details = {root_uri:root_node.serialize_no_none()} # Node info to show on click
    edge_data = [] # Edges for graph
    edge_details = {} # Edge info to show on click
    root_node_data = resource_to_node_data(root_node)
    include_same_as_name_only = kwargs.get("include_same_as_name_only",True)
    uris_to_ignore = set()

    if include_same_as_name_only is True:
        same_as_name_onlies = root_node.sameAsNameOnly.all()
        uris_to_ignore = set([x.uri for x in same_as_name_onlies])

    for rel_data in root_node.all_directional_relationships():
        build_out_graph_entries(rel_data, node_data, node_details, edge_data, edge_details, uris_to_ignore)

    if include_same_as_name_only is True:
        for org in same_as_name_onlies:
            for rel_data in org.all_directional_relationships(override_from_uri=root_uri):
                if rel_data["other_node"] not in same_as_name_onlies:
                    build_out_graph_entries(rel_data, node_data, node_details, edge_data, edge_details, uris_to_ignore)

    return node_data, edge_data, node_details, edge_details


def build_out_graph_entries(rel_data, node_data, node_details, edge_data, edge_details, uris_to_ignore):
    other_node = rel_data["other_node"]
    logger.info(f"From {rel_data['from_uri']} to {other_node.uri}")
    if other_node.uri in uris_to_ignore:
        logger.info(f"Ignoring {other_node}")
        return
    if not isinstance(other_node, IndustryCluster) and other_node.internalDocId is None:
        logger.info(f"Not showing external {other_node}")
    # Add this relationship and node to the graph
    next_edge = build_edge_vals(rel_data["direction"],rel_data["label"],rel_data["from_uri"],other_node.uri)
    next_edge_id = next_edge["id"]
    if next_edge_id in edge_details:
        logger.info(f"Already seen {next_edge_id}, ignoring")
        return
    edge_data.append(next_edge)
    edge_detail = {"from_uri":rel_data["from_uri"],"to_uri":other_node.uri,"relationship":rel_data["label"]}
    doc_extract = rel_data.get("document_extract")
    if doc_extract is not None:
        edge_detail["document_extract"] = doc_extract
    edge_details[next_edge_id] = edge_detail
    other_uri = other_node.uri
    if other_uri in node_details:
        logger.info(f"{other_uri} already seen, not adding")
        return
    node_details[other_uri] = other_node.serialize_no_none()
    node_data.append( resource_to_node_data(other_node))
    # find more relationships
    if not isinstance(other_node, Organization) and not isinstance(other_node, IndustryCluster):
        for rel_data in other_node.all_directional_relationships():
            build_out_graph_entries(rel_data, node_data, node_details, edge_data, edge_details, uris_to_ignore)


def build_edge_vals(direction, edge_label, source_node_id, target_node_id):
    if direction == "to":
        from_node_id = source_node_id
        to_node_id = target_node_id
        arrow_direction = "to"
    elif direction == "from":
        from_node_id = target_node_id
        to_node_id = source_node_id
        arrow_direction = "to"
    else:
        sorted_nodes = sorted([source_node_id,target_node_id])
        from_node_id = sorted_nodes[0]
        to_node_id = sorted_nodes[1]
        arrow_direction = "none"
    edge_color = EDGE_COLORS.get(edge_label)
    edge_vals = {"id": f"{from_node_id}-{to_node_id}-{edge_label}", "from": from_node_id, "to": to_node_id, "label": edge_label, "arrows": arrow_direction, "color": edge_color}
    return edge_vals


def resource_to_node_data(node):
    color, shape = node_color_shape(node)
    serialized = node.serialize_no_none()
    node_data = {"id":node.uri,"color":color,"shape":shape,"label":serialized.get("label",node.uri),"entity_type":serialized["entity_type"]}
    return node_data
