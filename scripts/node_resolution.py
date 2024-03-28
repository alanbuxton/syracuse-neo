from neomodel import db


def resolve_objects_with_error_handling(vals):
    '''
        vals, res = db.cypher_query(query)
        resolve_objects_with_error_handling(vals)
    '''
    resolved = []
    errors = []
    not_resolved = []
    for row in vals:
        assert isinstance(row,list), f"Expected {row} to be a list"
        try:
            tmp_row = row + [] # object_resolution seems to edit in place, this ensures we attempt to resolve a separate list
            res = db._object_resolution([tmp_row])
            resolved.append(res)
        except Exception as e:
            errors.append(e)
            not_resolved.append(tmp_row)
    return resolved, errors, not_resolved

def resolve_query_with_error_handling(query):
    return resolve_objects_with_error_handling(query)
