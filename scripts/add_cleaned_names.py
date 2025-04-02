'''
    Moving from storing a "sameAsNameOnly" relation to storing cleaned names and searching for same names
    This script will mass update based on
'''

from topics.models import Resource, Organization
import pickle
from neomodel import db
import logging
logger = logging.getLogger(__name__)

def main(input_file="tmp/cleaned_names.pickle"):
    with open(input_file, "rb") as f:
        data = pickle.load(f)
    for uri, vals in data.items():
        r = Resource.nodes.get_or_none(uri=uri)
        if r is None:
            logger.info(f"Couldn't find {uri}")
        if hasattr(r,"internalCleanedName") and r.internalCleanedName is not None:
            logger.info(f"{uri} already set")
            continue
        params = {"cleaned_name":vals['cleaned_name'],"cleaned_short_name":vals['cleaned_name_short']}
        query = f"""MATCH (n: Resource&Organization {{uri:'{uri}'}})
        SET n.internalCleanName = $cleaned_name
        SET n.internalCleanShortName = $cleaned_short_name
        """
        db.cypher_query(query,params)
        logger.info(f"Updated {uri}")
