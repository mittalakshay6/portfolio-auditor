import logging
import casparser
import json

logger = logging.getLogger(__name__)


def load_data(path: str, password: str) -> dict:
    return json.loads(casparser.read_cas_pdf(path, password, output="json"))


def batch_load_data(data_sources: list):
    logger.info("Loading data from CASs")
    cas_jsons = []
    for dataSource in data_sources:
        logger.info(f"Loading CAS for {dataSource['email']}...")
        cas_jsons.append(load_data(dataSource['path'], dataSource['password']))
    logger.info(f"Loaded {len(cas_jsons)} CASs")
    return cas_jsons
