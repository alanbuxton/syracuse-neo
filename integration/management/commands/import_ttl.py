from django.core.management.base import BaseCommand
import os
import subprocess
from integration.models import DataImport
from datetime import datetime, timezone
from integration.neo4j_utils import (
    setup_db_if_necessary, get_node_name_from_rdf_row,
    get_internal_doc_ids_from_rdf_row, count_nodes,
    flag_doc_ids_for_adding_to_typesense,
    flag_doc_ids_for_removal_from_typesense,
)
import time
import logging
from neomodel import db
from trackeditems.management.commands.send_recent_activities_email import do_send_recent_activities_email
from syracuse.settings import RDF_SLEEP_TIME, RDF_DUMP_DIR, RDF_ARCHIVE_DIR
from pathlib import Path
from topics.cache_helpers import refresh_geo_data
from integration.rdf_post_processor import RDFPostProcessor
from auth_extensions.anon_user_utils import create_anon_user
from integration.neo4j_utils import delete_and_clean_up_nodes_by_doc_id 

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

def load_ttl_files(dir_name,RDF_SLEEP_TIME,
                    raise_on_error=True):
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
        return 0, count_of_deletions
    for filename in all_files:
        creations = load_file(f"{dir_name}/{filename}",RDF_SLEEP_TIME,raise_on_error)
        count_of_creations += creations
    logger.info(f"After running insertion files there are {count_nodes()} nodes")
    return count_of_creations, count_of_deletions

def load_deletion_file(filepath):
    filepath = os.path.abspath(filepath)
    cnt = count_nodes()
    doc_ids = set()
    with open(filepath) as f:
        for row in f.readlines():
            doc_id = get_internal_doc_ids_from_rdf_row(row)
            if doc_id is None:
                continue
            doc_ids.add(doc_id)
            delete_and_clean_up_nodes_by_doc_id(doc_id)
    flag_doc_ids_for_removal_from_typesense(doc_ids)
    cnt2 = count_nodes()
    logger.info(f"Before deleting {cnt} nodes. After delete {filepath} {cnt2} nodes")
    return cnt2 - cnt

def load_file(filepath,RDF_SLEEP_TIME, raise_on_error=True):
    cnt = count_nodes()
    filepath = os.path.abspath(filepath)
    command = f"""CALL n10s.rdf.import.fetch("file://{filepath}","Turtle",
                {{ predicateExclusionList : [ "https://1145.am/db/geoNamesRDF" ] }} );"""
    logger.info(f"Loading: {command}")
    results,_ = db.cypher_query(command)
    res = results[0] # row like ['KO', 0, 5025, None, 'Unexpected character U+FFFC at index 57: https://1145.am/db/techcrunchcom_2011_12_02_doo-net-gets-￼6-8m-to-reinvent-office-paperwork-oh-yes-2_ [line 5121]', {'singleTx': False}]
    if res[0] != 'OK':
        logger.error(f"{command}: {res}")
        if raise_on_error is True:
            raise ValueError(f"{command} failed with {res}")
    doc_ids = set()
    uris = set()
    with open(filepath) as f:
        for row in f.readlines():
            doc_id = get_internal_doc_ids_from_rdf_row(row)
            uri = get_node_name_from_rdf_row(row)
            if doc_id is not None:
                doc_ids.add(doc_id)
            if uri is not None:
                uris.add(uri)
    flag_doc_ids_for_adding_to_typesense(doc_ids)
    cnt2 = count_nodes()
    time.sleep(RDF_SLEEP_TIME)
    logger.info(f"Before importing {cnt} nodes. After importing {filepath} {cnt2} nodes")
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
        parser.add_argument("-g","--do_post_processing",
                default=True,
                action="store_false",
                help="Set this flag to not auto-run post-processing")
        parser.add_argument("-r","--raise_on_error",
                default=True,
                action="store_false",
                help="Set this flag to continue processing imports even if the load process fails")
        parser.add_argument("-a","--do_archiving",
                default=True,
                action="store_false",
                help="Set this flag to disable archiving")
        parser.add_argument("-d","--dirname",
                default=RDF_DUMP_DIR,
                help="Set dump directory")
        parser.add_argument("-o","--only_post_processing",
                default=False,
                action="store_true",
                help="Just run post-processing (excluding stats calculation)")

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
    raise_on_error = options.get("raise_on_error",True)
    R = RDFPostProcessor()
    if force:
        cleanup(pidfile)
    if not is_allowed_to_start(pidfile):
        logger.info("Already running, or previous run did not shut down cleanly, not spawning new run")
        return None
    if options.get("only_post_processing",False) is True:
        logger.info("Only doing post processing")
        R.run_all_in_order()
        R.run_typesense_update()
        cleanup(pidfile)
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
                                                    raise_on_error=raise_on_error)
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
        R.run_all_in_order()
        _ = refresh_geo_data()
        R.run_typesense_update()
    if send_notifications is True and total_creations > 0:
        do_send_recent_activities_email()
    else:
        logger.info("No email sending this time")
    logger.info("re-set cache")
    create_anon_user()
    cleanup(pidfile)
