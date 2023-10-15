import os
import argparse
from syracuse.settings import NEOMODEL_NEO4J_BOLT_URL
from neomodel import db, config
from datetime import datetime
import logging
from typing import List

'''
    Run this script at command line to import all *.ttl files from a source directory e.g.
    `python import_export path/to/ttl/files`

    To export a subset of data as cypher call `export_data_subset`

    Export/import requires the following to be set in `apoc.conf`:

    apoc.export.file.enabled=true
    apoc.import.file.enabled=true
    apoc.import.file.use_neo4j_config=false
'''

config.DATABASE_URL = NEOMODEL_NEO4J_BOLT_URL
logger = logging.getLogger("syracuse")
log_level = os.environ.get("LOG_LEVEL","INFO")
logger.setLevel(log_level)
logger.addHandler(logging.StreamHandler())

def load_ttl_files(dir_name):
    all_files = [x for x in os.listdir(dir_name) if x.endswith(".ttl")]
    logger.info(f"Found {len(all_files)} ttl files to process")
    if len(all_files) == 0:
        return
    for filename in all_files:
        load_file(f"{dir_name}/{filename}")
    apoc_del_redundant_high_med()

def load_file(filepath):
    filepath = os.path.abspath(filepath)
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


def export_data_subset(limit = 9_000, output_dir = f"{os.getcwd()}/tmp"):
    '''
        import with
        cat /foo/bar/export.cypher | ./bin/cypher-shell -u neo4j -p <password>
    '''
    uris = get_sample_uris(limit)
    return export_subset_by_uris(uris, output_dir)


def get_sample_uris(limit = 100):
    role_uris, _ = db.cypher_query(f"MATCH (n: Organization)--(o: Role)--(p: RoleActivity)--(q: Person) RETURN n.uri,o.uri,p.uri,q.uri LIMIT {limit}")
    corp_fin_uris, _ = db.cypher_query(f"MATCH (n: Organization)--(o: CorporateFinanceActivity)--(p: Organization) RETURN n.uri, o.uri, p.uri LIMIT {limit}")
    loc_uris, _ = db.cypher_query(f"MATCH (n: Organization)--(o: LocationActivity)--(p: Site) RETURN n.uri, o.uri, p.uri LIMIT {limit}")
    flattened = [x for sublist in role_uris + corp_fin_uris + loc_uris for x in sublist]
    return flattened

def export_subset_by_uris(uris: List[str], output_dir: str):
    query = f'''MATCH path = (r: Resource)-[s]-(t) WHERE r.uri in {uris}
        WITH apoc.coll.toSet(collect(r)+collect(t)) as export_nodes, apoc.coll.toSet(collect(s)) as export_rels
        CALL apoc.export.cypher.data(export_nodes,export_rels,"{output_dir}/export.cypher",{{format:'cypher-shell'}})
        YIELD file, source, format, nodes, relationships, properties, time
        RETURN nodes, relationships, time;'''
    logger.info(f"Will run: {query}")
    exported = db.cypher_query(query)
    logger.info(exported)
    return exported


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "dir_name",
        help="Directory containing ttl files.",
    )
    args = parser.parse_args()
    load_ttl_files(args.dir_name)
