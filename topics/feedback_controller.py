import os
import csv
from urllib.parse import urlparse
import uuid

FEEDBACK_FNAME = "tmp/feedback.csv"

def write_csv(row, fname):
    with open(fname, "a") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def store_feedback(node_or_edge, unique_id, reason):
    if node_or_edge == 'node':
        doc_id = doc_id_from_uri(unique_id)
        parts = [unique_id,None,None]
    elif node_or_edge == 'edge':
        source, target, relationship = unique_id.split("-")
        doc_id = doc_id_from_uri(source)
        parts = [source,target, relationship]
    else:
        return None, f"Didn't expect {node_or_edge} for {unique_id}"
    feedback_id = str(uuid.uuid4())
    row = [feedback_id,node_or_edge,doc_id] + parts + [reason]
    try:
        write_csv(row, FEEDBACK_FNAME)
        return feedback_id , None
    except Exception as e:
        return None, str(e)


def doc_id_from_uri(uri):
    parts = urlparse(uri)
    if parts.netloc == '1145.am':
        doc_id = parts.path.split("/")[2] # Format is 1145.am/db/<doc_id>/etc
    else:
        doc_id = None
    return doc_id
