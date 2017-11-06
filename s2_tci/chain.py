import logging

import tqdm
import sentinelsat

from s2_tci import query
from s2_tci import find
from s2_tci import download

logger = logging.getLogger(__name__)


def get_tci(username, password, area_geom, outdir, **query_kw):
    api = sentinelsat.SentinelAPI(user=username, password=password)
    session = api.session
    logger.info('Querying SciHub')
    results = query.query_s2(api, area_geom, **query_kw)
    logger.info('Found %d products', len(results))
    results_iter = tqdm.tqdm(results.values(), desc='Retrieving TCI URLs', unit='result')
    urls = find.get_tci_urls(results_iter, s=session)
    logger.info('Retrieved %d TCI download URLs', len(urls))
    url_iter = tqdm.tqdm(urls, desc='Downloading TCI files', unit='file')
    targets = download.download_urls(url_iter, outdir=outdir, s=session)
    logger.info('Downloaded %d files', len(targets))
    return targets
