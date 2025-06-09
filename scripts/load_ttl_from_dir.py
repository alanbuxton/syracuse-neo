from integration.management.commands.import_ttl import load_file
import os 
import logging
logger = logging.getLogger(__name__)

def load_ttl_files(dirname):
    for fname in os.listdir(dirname):
        if not fname.endswith(".ttl"):
            logger.info(f"{fname} is not a ttl file, skipping")
            continue
        load_file(f"{dirname}/{fname}",0,True)



