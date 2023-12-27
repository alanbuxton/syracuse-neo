import logging
from neomodel import db
import re
from datetime import datetime

logger = logging.getLogger(__name__)

def setup_db_if_necessary():
    db.cypher_query("CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE;")
    v, _ = db.cypher_query("call n10s.graphconfig.show")
    if len(v) == 0:
        multivals = ["actionFoundName","activityType","basedInHighGeoName",
                    "basedInHighRaw","basedInLowRaw",
                    "description","foundName","industry",
                    "locationFoundName",
                    "locationPurpose","locationType","name","orgFoundName",
                    "roleFoundName","roleHolderFoundName",
                    "status","targetDetails","targetName","valueRaw",
                    "when","whenRaw","whereGeoName","whereRaw"]
        proplist = [f"https://1145.am/db/{x}" for x in multivals]
        query = 'CALL n10s.graphconfig.init({handleVocabUris: "MAP",handleMultival:"ARRAY",multivalPropList:["' + "\",\"".join(proplist) + '"]})';
        logger.info(query)
        db.cypher_query(query)

def apoc_del_redundant_high_med():
    output_stats("Before delete")
    apoc_query_medium = f'CALL apoc.periodic.iterate("MATCH (n1:Organization)-[r1:sameAsMedium]->(n2:Organization)-[r2:sameAsMedium]->(n1) where elementId(n1) < elementId(n2) RETURN *","DELETE r2",{{}})'
    db.cypher_query(apoc_query_medium)
    output_stats("After Delete sameAsMedium")
    apoc_query_high = f'CALL apoc.periodic.iterate( "MATCH (n1:Organization)-[r1:sameAsHigh]->(n2:Organization)-[r2:sameAsHigh]->(n1) where elementId(n1) < elementId(n2) RETURN *","DELETE r2",{{}})'
    db.cypher_query(apoc_query_high)
    output_stats("After Delete sameAsHigh")

def output_stats(msg):
    high = "MATCH (n1)-[r:sameAsHigh]-(n2)"
    medium = "MATCH (n1)-[r:sameAsMedium]-(n2)"
    same_as_high_count,_ = db.cypher_query(high + " RETURN COUNT(r)")
    same_as_medium_count,_ = db.cypher_query(medium + " RETURN COUNT(r)")
    logger.info(f"{datetime.utcnow()} {msg} sameAsHigh: {same_as_high_count[0][0]}; sameAsMedium: {same_as_medium_count[0][0]}")

def get_node_name_from_rdf_row(row):
    res = re.findall(r"^<(https://\S.+)> a", row)
    if len(res) > 0:
        return res[0]
    else:
        return None

def count_nodes():
    val, _ = db.cypher_query("MATCH (n) RETURN COUNT(n)")
    return val[0][0]
