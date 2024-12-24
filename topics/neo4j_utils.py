from datetime import datetime
import neo4j

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
                new_row.append(datetime.fromisoformat(item.isoformat()))
            else:
                new_row.append(item)
        new_vals.append(new_row)
    return new_vals