import logging
import functools
import concurrent.futures
from contextlib import closing

import tqdm
import sentinelsat

from s2_tci import query
from s2_tci import find
from s2_tci import download

logger = logging.getLogger(__name__)


def download_tci(username, password, area_geom, outdir, **query_kw):
    api = sentinelsat.SentinelAPI(user=username, password=password)
    session = api.session

    logger.info('Querying SciHub')
    results = query.query_s2(api, area_geom, **query_kw)
    logger.info('Found %d products', len(results))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results_iter = tqdm.tqdm(results.values(), desc='Retrieving TCI URLs', unit='result')
        urls = executor.map(functools.partial(find.get_tci_url, session=session), results_iter)
        urls = [url for url in urls if url is not None]
        logger.info('Retrieved %d TCI download URLs', len(urls))

        url_iter = tqdm.tqdm(urls, desc='Downloading TCI files', unit='file')
        targets = executor.map(functools.partial(download.download_file, outdir=outdir, session=session), url_iter)
        logger.info('Downloaded %d files', len(targets))

    return targets


def stream_tci(username, password, area_geom, outdir, **query_kw):
    api = sentinelsat.SentinelAPI(user=username, password=password)

    logger.info('Querying SciHub')
    results = query.query_s2(api, area_geom, **query_kw)
    logger.info('Found %d products', len(results))

    with closing(api.session) as session:
        url_generator = (find.get_tci_url(res, session=session) for res in results.values())
        for url in url_generator:
            yield download.stream_file(url, session=session)
