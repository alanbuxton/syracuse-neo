import os
import argparse
from syracuse.settings import NEOMODEL_NEO4J_BOLT_URL
from neomodel import db, config
from datetime import datetime
import logging
from typing import List

'''
    Run this script at command line to import all *.ttl files from a source directory

    To export a subset of data as cypher call `export_data_subset`
'''

config.DATABASE_URL = NEOMODEL_NEO4J_BOLT_URL
logger = logging.getLogger("syracuse")

def load_ttl_files(dir_name):
    for filename in os.listdir(dir_name):
        if not filename.endswith(".ttl"):
            continue
        load_file(f"{dir_name}/{filename}")
    apoc_del_redundant_high_med()

def load_file(filepath):
    command = f'call n10s.rdf.import.fetch("file://{filepath}","Turtle");'
    logger.info(f"Loading: {command}")
    db.cypher_query(command)


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


def export_data_subset(org_limit = 100, output_dir = f"{os.getcwd()}/tmp"):
    '''
        import with
        cat /foo/bar/export.cypher | ./bin/cypher-shell -u neo4j -p <password>
    '''
    org_uris, _ = db.cypher_query(f"MATCH (n:Organization)-[]-(:CorporateFinanceActivity) RETURN n.uri ORDER BY elementId(n) DESC LIMIT {org_limit}")
    flattened_uris = [x for sublist in org_uris for x in sublist]
    export_orgs_by_uri(flattened_uris, output_dir)


def export_orgs_by_uri(org_uris: List[str], output_dir: str):
    '''
        Given a list of org uris, export them and 2 steps away: Org - Activity - Target
    '''
    query = f'''MATCH path = (o1:Organization)-[r1]->(o2)-[r2]->(o3)
        WHERE o1.uri in {org_uris}
        WITH apoc.coll.toSet(collect(o1)+collect(o2)+collect(o3)) as export_nodes, apoc.coll.toSet(collect(r1)+collect(r2)) as export_rels
        CALL apoc.export.cypher.data(export_nodes,export_rels,"{output_dir}/export.cypher",{{format:'cypher-shell'}})
        YIELD file, source, format, nodes, relationships, properties, time
        RETURN nodes, relationships, time;'''
    logger.info(f"Will run: {query}")
    exported = db.cypher_query(query)
    logger.info(exported)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "dir_name",
        help="Directory containing ttl files.",
    )
    args = parser.parse_args()
    load_ttl_files(args.dir_name)
