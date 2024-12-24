# Mainly made by ChatGPT (4o mini)

def filtered_hierarchy(tree, relevant_values):
    return sort_leaves(filter_tree(tree, relevant_values))

def hierarchy_widths(d):
 
    # Function to calculate the width for each key and store it
    def find_widths(d, prefix=""):
        if isinstance(d, dict):
            for key, value in d.items():
                current_key = f"{prefix}{key}" if prefix == "" else f"{prefix}#{key}"
                # If value is a list, we calculate the width for that key
                if isinstance(value, list):
                    value_len = len(value)
                    widths[current_key] = value_len
                else:
                    # Otherwise, recursively calculate the width for nested dictionaries
                    find_widths(value, current_key)

    # Now we calculate the width of each key, aggregating the widths of sub-keys
    def aggregate_widths():
        for key in list(widths.keys())[::-1]:  # Process from the deepest key upwards
            parts = key.split('#')
            for i in range(len(parts) - 1, 0, -1):
                parent_key = '#'.join(parts[:i])
                if parent_key not in widths:
                    widths[parent_key] = 0
                widths[parent_key] += widths[key]

    widths = {}
    find_widths(d)
    aggregate_widths()
    return widths

    
def sort_leaves(tree):
    if isinstance(tree, dict):
        return {k: sort_leaves(v) for k, v in tree.items()}
    elif isinstance(tree, set) or isinstance(tree, list):
        return sorted(tree)
    else:
        return tree

def filter_tree(tree, relevant_values):
    """
    Filters the tree structure to keep only the relevant end nodes from the relevant_values list.

    Args:
        tree (dict): The nested dictionary to filter.
        relevant_values (set): A set of relevant values to keep in the end nodes.

    Returns:
        dict: A filtered dictionary.
    """
    if isinstance(tree, dict):
        # If the current node is a dictionary, apply the filtering recursively
        return {key: filter_tree(value, relevant_values) 
                for key, value in tree.items() if filter_tree(value, relevant_values)}
    elif isinstance(tree, list):
        # If the current node is a list, keep only the values that are in the relevant values
        return [item for item in tree if item in relevant_values]
    else:
        # If the node is not a list or dict, return the item itself
        return tree

