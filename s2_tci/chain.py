import logging
import functools
import concurrent.futures
import threading
from contextlib import closing

import tqdm
import sentinelsat

from s2_tci import query
from s2_tci import find
from s2_tci import download
from s2_tci import generator

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
        targets = executor.map(
            functools.partial(download.download_file, outdir=outdir, session=session), url_iter)
        logger.info('Downloaded %d files', len(targets))

    return targets


class MaxSizeThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """Simple subclass of ProcessPoolExecutor that has a maximum queue size"""
    def __init__(self, queue_size, *args, **kwargs):
        super(MaxSizeThreadPoolExecutor, self).__init__(*args, **kwargs)
        self._semaphore = threading.Semaphore(queue_size)

    def release(self, future):
        self._semaphore.release()

    def submit(self, *args, **kwargs):
        self._semaphore.acquire()
        future = super(MaxSizeThreadPoolExecutor, self).submit(*args, **kwargs)
        future.add_done_callback(self.release)
        return future


class stream_tci(generator.GeneratorWithLength):
    def __init__(self, username, password, area_geom, max_workers=2, **query_kw):
        api = sentinelsat.SentinelAPI(user=username, password=password, show_progressbars=False)
        results = query.query_s2(api, area_geom, **query_kw)
        self._length = len(results)

        def _get_and_download_file(base_url, session):
            product_url = find.get_tci_url(base_url, session=session)
            fname = download.fname_from_url(product_url)

            return download.stream_file(product_url, session=session)

        def data_generator():
            with MaxSizeThreadPoolExecutor(queue_size=max_workers, max_workers=max_workers) as executor:
                with closing(api.session) as session:
                    pending = set()
                    for url in results.values():
                        future = executor.submit(_get_and_download_file, url, session)
                        pending.add(future)

                        done, pending = concurrent.futures.wait(pending, timeout=0)
                        for task in done:
                            yield task.result()

                    done, _ = concurrent.futures.wait(pending)
                    for task in done:
                        yield task.result()

        self._generator = data_generator
