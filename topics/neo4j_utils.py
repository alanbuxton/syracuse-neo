from datetime import datetime
import neo4j
import re

def date_to_cypher_friendly(date):
    if isinstance(date, str):
        return datetime.fromisoformat(date).isoformat()
    else:
        return date.isoformat()
    
def neo4j_date_converter(vals):
    new_vals=[]
    for row in vals:
        new_row = []
        for item in row:
            if isinstance(item, neo4j.time.DateTime):
                new_row.append(neo4j_to_datetime(item))
            else:
                new_row.append(item)
        new_vals.append(new_row)
    return new_vals

def neo4j_to_datetime(item):
    if item is None:
        return None
    return datetime.fromisoformat(item.isoformat())

def clean_str(text):
    return re.sub(r"""(['"])""", r"\\\1", text)