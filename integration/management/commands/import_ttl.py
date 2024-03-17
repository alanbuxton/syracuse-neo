from django.core.management.base import BaseCommand
import os
import subprocess
from integration.models import DataImport
from datetime import datetime, timezone
from integration.neo4j_utils import (
    setup_db_if_necessary,
    get_node_name_from_rdf_row, count_nodes
)
import time
import logging
from neomodel import db
from trackeditems.management.commands.send_recent_activities_email import do_send_recent_activities_email
from topics.cache_helpers import rebuild_cache
from syracuse.settings import RDF_SLEEP_TIME, RDF_DUMP_DIR, RDF_ARCHIVE_DIR
from pathlib import Path
from integration.merge_nodes import post_import_merging

logger = logging.getLogger(__name__)
PIDFILE="/tmp/syracuse-import-ttl.pid"

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

def move_files(src_dir,target_dir):
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    res = subprocess.run(['mv',src_dir,target_dir])
    logger.info(res)

def load_ttl_files(dir_name,RDF_SLEEP_TIME,do_post_processing=True):
    delete_dir = f"{dir_name}/deletions"
    count_of_creations = 0
    count_of_deletions = 0
    if os.path.isdir(delete_dir):
        delete_files = [x for x in os.listdir(delete_dir) if x.endswith(".ttl")]
        logger.info(f"Found {len(delete_files)} ttl files to delete, currenty have {count_nodes()} nodes")
        for filename in delete_files:
            _, deletions = load_deletion_file(f"{delete_dir}/{filename}")
            count_of_deletions += deletions
    logger.info(f"After running deletion files there are {count_nodes()} nodes")
    all_files = sorted([x for x in os.listdir(dir_name) if x.endswith(".ttl")])
    logger.info(f"Found {len(all_files)} ttl files to process")
    if len(all_files) == 0:
        logger.info("No insertion files to load, quitting")
        return count_of_deletions, 0
    for filename in all_files:
        creations = load_file(f"{dir_name}/{filename}",RDF_SLEEP_TIME)
        count_of_creations += creations
    logger.info(f"After running insertion files there are {count_nodes()} nodes")
    if do_post_processing is True:
        post_import_merging()
    return count_of_creations, count_of_deletions

def load_deletion_file(filepath):
    filepath = os.path.abspath(filepath)
#    command = f'call n10s.rdf.delete.fetch("file://{filepath}","Turtle");' # This fails for me, but not clear why
    with open(filepath) as f:
        uris = [get_node_name_from_rdf_row(x) for x in f.readlines() if get_node_name_from_rdf_row(x) is not None]
    count_query = f"MATCH (n) WHERE n.uri IN {uris} RETURN COUNT(n)"
    cnt, _ = db.cypher_query(count_query)
    command = f"MATCH (n) WHERE n.uri IN {uris} AND n.internalDocIdList IS NULL CALL apoc.nodes.delete(n, 1000) YIELD value RETURN value;"
    logger.info(f"Deleting {len(uris)} nodes")
    db.cypher_query(command)
    return len(uris), cnt[0][0]

def load_file(filepath,RDF_SLEEP_TIME):
    filepath = os.path.abspath(filepath)
    with open(filepath) as f:
        uris = [get_node_name_from_rdf_row(x) for x in f.readlines() if get_node_name_from_rdf_row(x) is not None]
    command = f'call n10s.rdf.import.fetch("file://{filepath}","Turtle");'
    logger.info(f"Loading: {command}")
    db.cypher_query(command)
    time.sleep(RDF_SLEEP_TIME)
    return len(uris)

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("-p","--pidfile",
                default=PIDFILE,
                help=f"Defaults to {PIDFILE}")
        parser.add_argument("-f","--force",
                default=False,
                action="store_true",
                help="Set this if you want to ignore any pre-existing pidfile")
        parser.add_argument("-n","--send_notifications",
                default=False,
                action="store_true",
                help="Send notification emails after successful import")

    def handle(self, *args, **options):
        do_import_ttl(**options)

def do_import_ttl(**options):
    logger.info("Started import_ttl")
    pidfile = options.get("pidfile",PIDFILE)
    force = options.get("force",False)
    dump_dir = options.get("dirname",RDF_DUMP_DIR)
    do_archiving = options.get("do_archiving",True)
    send_notifications = options.get("send_notifications",False)
    do_post_processing = options.get("do_post_processing",True)
    if force:
        cleanup(pidfile)
    if not is_allowed_to_start(pidfile):
        logger.info("Already running, or previous run did not shut down cleanly, not spawning new run")
        return None
    export_dirs = new_exports_to_import(dump_dir)
    if len(export_dirs) == 0:
        logger.info("No new TTL files to import")
        cleanup(pidfile)
        return None
    setup_db_if_necessary()
    total_creations = 0
    total_deletions = 0
    for export_dir in export_dirs:
        count_of_creations, count_of_deletions = load_ttl_files(
                                                    export_dir,RDF_SLEEP_TIME,
                                                    do_post_processing=do_post_processing)
        di = DataImport(
            run_at = datetime.now(tz=timezone.utc),
            import_ts = os.path.basename(export_dir),
            deletions = count_of_deletions,
            creations = count_of_creations,
        )
        total_creations += count_of_creations
        total_deletions += count_of_deletions
        di.save()
        if do_archiving is True:
            logger.info(f"Archiving files from {export_dir} to {RDF_ARCHIVE_DIR}")
            move_files(export_dir,RDF_ARCHIVE_DIR)
    logger.info(f"Loaded {total_creations} creations and {total_deletions} deletions from {len(export_dirs)} directories")
    if do_post_processing is True:
        post_import_merging(with_delete_not_needed_resources=True)
        rebuild_cache()
    logger.info("re-set cache")
    cleanup(pidfile)
    if send_notifications is True and total_creations > 0:
        do_send_recent_activities_email()
    else:
        logger.info("No email sending this time")
