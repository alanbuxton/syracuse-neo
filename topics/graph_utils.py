import re

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


def node_and_neighbors(source_node):
    node_data = []
    edge_data = []
    nodes = []

    val = source_node.serialize()
    val["id"] = val["uri"]
    (color, shape) = NODE_COLOR_SHAPES.get(val["entity_type"],("#defa9d","triangleDown"))
    node_vals = {**val, **{"color": color, "shape": shape}}
    node_data.append(node_vals)

    rels = source_node.all_relationships()
    for rel_type, direction, rel_node in rels:
        val = rel_node.serialize()
        val["id"] = val["uri"]
        (color, shape) = NODE_COLOR_SHAPES.get(val["entity_type"],("#defa9d","triangleDown"))
        node_vals = {**val, **{"color": color, "shape": shape}}
        if direction == "to":
            from_node = source_node
            to_node = rel_node
            arrow_direction = "to"
        elif direction == "from":
            from_node = rel_node
            to_node = source_node
            arrow_direction = "to"
        else:
            sorted_nodes = sorted([source_node,rel_node],key=lambda x: x.uri)
            from_node = sorted_nodes[0]
            to_node = sorted_nodes[1]
            arrow_direction = "none"
        edge_color = EDGE_COLORS.get(rel_type)
        edge_vals = {"from": from_node.uri, "to": to_node.uri, "label": rel_type_to_edge_label(rel_type), "arrows": arrow_direction, "color": edge_color}
        node_data.append(node_vals)
        edge_data.append(edge_vals)
        nodes.append(rel_node)

    return node_data, edge_data, nodes


def graph_to_depth(source_node, max_depth=3):
    idx = 0
    all_nodes = []
    all_edges = []
    node_details = {source_node.uri: source_node.serialize_no_none()}
    nodes = [source_node]
    while idx < max_depth:
        tmp_new_nodes = []
        for node in nodes:
            node_data, edge_data, new_nodes = node_and_neighbors(node)
            all_nodes.extend(node_data)
            all_edges.extend(edge_data)
            tmp_new_nodes.extend(new_nodes)
            for new_node in new_nodes:
                node_details[new_node.uri] = new_node.serialize_no_none()
        idx += 1
        nodes=tmp_new_nodes
    return all_nodes, all_edges, node_details


def rel_type_to_edge_label(text):
    text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).upper()
    text = re.sub("_GEO_NAME_","_GEONAME_",text)
    text = re.sub("R_D_F","RDF",text)
    return text
