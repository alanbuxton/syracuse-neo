from django.core.management.base import BaseCommand
import os
from integration.models import DataImport
from datetime import datetime, timezone
from integration.management.commands._neo4j_utils import (
    setup_db_if_necessary, apoc_del_redundant_high_med,
    get_node_name_from_rdf_row, count_nodes
)
import time
import logging
from neomodel import db
logger = logging.getLogger(__name__)

PIDFILE="/tmp/syracuse-import-ttl.pid"
SLEEP_TIME=0
DUMP_DIR="tmp/dump"

def is_running(pidfile):
    if os.path.exists(pidfile):
        return True
    else:
        return False

def is_allowed_to_start(pidfile):
    if is_running(pidfile):
        return False
    else:
        with open(pidfile, "w", encoding='utf-8') as f:
            f.write(str(os.getpid()))
        return True

def cleanup(pidfile):
    if os.path.isfile(pidfile):
        os.remove(pidfile)

def new_exports_to_import(dirname):
    latest_ts = DataImport.latest_import()
    if latest_ts is None:
        latest_ts = 1
    export_dirs = []
    for x in sorted(os.listdir(dirname)):
        try:
            if int(x) > latest_ts:
                export_dirs.append(os.path.join(dirname,x))
        except ValueError:
            logger.debug(f"Can't convert {x} to int")
    return export_dirs

def load_ttl_files(dir_name,sleep_time):
    delete_dir = f"{dir_name}/deletions"
    count_of_creations = 0
    count_of_deletions = 0
    if os.path.isdir(delete_dir):
        delete_files = [x for x in os.listdir(delete_dir) if x.endswith(".ttl")]
        logger.info(f"Found {len(delete_files)} ttl files to delete, currenty have {count_nodes()} nodes")
        for filename in delete_files:
            deletions = load_deletion_file(f"{delete_dir}/{filename}")
            count_of_deletions += deletions
    logger.info(f"After running deletion files there are {count_nodes()} nodes")
    all_files = sorted([x for x in os.listdir(dir_name) if x.endswith(".ttl")])
    logger.info(f"Found {len(all_files)} ttl files to process")
    if len(all_files) == 0:
        logger.info("No insertion files to load, quitting")
        return
    for filename in all_files:
        creations = load_file(f"{dir_name}/{filename}",sleep_time)
        count_of_creations += creations
    logger.info(f"After running insertion files there are {count_nodes()} nodes")
    apoc_del_redundant_high_med()
    return count_of_creations, count_of_deletions

def load_deletion_file(filepath):
    filepath = os.path.abspath(filepath)
#    command = f'call n10s.rdf.delete.fetch("file://{filepath}","Turtle");' # This fails for me, but not clear why
    with open(filepath) as f:
        uris = [get_node_name_from_rdf_row(x) for x in f.readlines() if get_node_name_from_rdf_row(x) is not None]
    command = f"match (n) where n.uri in {uris} detach delete n"
    logger.info(f"Deleting {len(uris)} nodes")
    db.cypher_query(command)
    return len(uris)

def load_file(filepath,sleep_time):
    filepath = os.path.abspath(filepath)
    with open(filepath) as f:
        uris = [get_node_name_from_rdf_row(x) for x in f.readlines() if get_node_name_from_rdf_row(x) is not None]
    command = f'call n10s.rdf.import.fetch("file://{filepath}","Turtle");'
    logger.info(f"Loading: {command}")
    db.cypher_query(command)
    time.sleep(sleep_time)
    return len(uris)

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("-d","--dirname",
                default=DUMP_DIR,
                help=f"Defaults to {DUMP_DIR}. Expects one subdirectory per set of files to be imported named with YYYYMMDDHHMMSS numerical timestamp")
        parser.add_argument("-p","--pidfile",
                default=PIDFILE,
                help=f"Defaults to {PIDFILE}")
        parser.add_argument("-s","--sleep_time",
                default=0,type=int,
                help="Set this if you want to slow down imports to avoid overloading server")
        parser.add_argument("-f","--force",
                default=False,
                action="store_true",
                help="Set this if you want to ignore any pre-existing pidfile")

    def handle(self, *args, **options):
        pidfile = options.get("pidfile",PIDFILE)
        sleep_time = options.get("sleep_time",0)
        dirname = options.get("dirname",DUMP_DIR)
        force = options.get("force",False)
        if force:
            cleanup(pidfile)
        if not is_allowed_to_start(pidfile):
            logger.info("Already running, or previous run did not shut down cleanly, not spawning new run")
            return None
        export_dirs = new_exports_to_import(dirname)
        if len(export_dirs) == 0:
            logger.info("No new TTL files to import")
            return None
        setup_db_if_necessary()
        for export_dir in export_dirs:
            count_of_creations, count_of_deletions = load_ttl_files(
                                                        export_dir,sleep_time)
            di = DataImport(
                run_at = datetime.now(tz=timezone.utc),
                import_ts = os.path.basename(export_dir),
                deletions = count_of_deletions,
                creations = count_of_creations,
            )
            di.save()
        cleanup(pidfile)
