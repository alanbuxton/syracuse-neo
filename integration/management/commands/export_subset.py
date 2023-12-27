from django.core.management.base import BaseCommand
import os
import logging
from neomodel import db
from typing import List

logger = logging.getLogger(__name__)


def export_data_subset(limit = 9_000, output_dir = f"{os.getcwd()}/tmp"):
    '''
        import with
        cat /foo/bar/export.cypher | ./bin/cypher-shell -u neo4j -p <password>
    '''
    uris = get_sample_uris(limit)
    logger.info(f"Exporting to {output_dir}")
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

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("-l","--limit",
                default=9000,
                type=int,
                )

    def handle(self, *args, **options):
        export_data_subset(limit=options["limit"])
