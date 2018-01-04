import logging
import functools
import concurrent.futures
from contextlib import closing
import threading

import tqdm
import sentinelsat

from s2_tci import query
from s2_tci import find
from s2_tci import download
from s2_tci import generator, executor

logger = logging.getLogger(__name__)


def download_tci(username, password, area_geom, outdir, **query_kw):
    api = sentinelsat.SentinelAPI(user=username, password=password)
    session = api.session

    logger.info('Querying SciHub')
    results = query.query_s2(api, area_geom, **query_kw)
    logger.info('Found %d products', len(results))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as exe:
        results_iter = tqdm.tqdm(results.values(), desc='Retrieving TCI URLs', unit='result')
        urls = exe.map(functools.partial(find.get_tci_url, session=session), results_iter)
        urls = [url for url in urls if url is not None]
        logger.info('Retrieved %d TCI download URLs', len(urls))

        url_iter = tqdm.tqdm(urls, desc='Downloading TCI files', unit='file')
        targets = exe.map(
            functools.partial(download.download_file, outdir=outdir, session=session), url_iter)
        logger.info('Downloaded %d files', len(targets))

    return targets


class stream_tci(generator.GeneratorWithLength):
    def __init__(self, username, password, area_geom, exclude=None, **query_kw):
        api = sentinelsat.SentinelAPI(user=username, password=password, show_progressbars=False)
        results = query.query_s2(api, area_geom, **query_kw)
        self._length = len(results)

        exclude = set(exclude or [])

        def _get_and_download_file(base_url):
            product_url = find.get_tci_url(base_url, session=api.session)
            if product_url is None:
                return None, None

            fname = download.fname_from_url(product_url)
            if fname in exclude:
                return None, fname

            return download.stream_file(product_url, session=api.session), fname

        def _data_generator():
            with executor.MaxSizeThreadPoolExecutor(queue_size=4, max_workers=1) as exe:
                with closing(api.session):
                    yield from exe.map(_get_and_download_file, results.values())

        self._generator = _data_generator
