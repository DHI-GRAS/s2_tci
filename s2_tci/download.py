import os
import re
import shutil


def _fname_from_url(url):
    try:
        return re.search(r'([\w_]*?_TCI.jp2)', url).group(1)
    except AttributeError:
        raise ValueError('Unable to get TCI file name from URL "{}".'.format(url))


def _download_file(url, target, session):
    target_incomplete = target + '.incomplete'
    with session.get(url, stream=True) as response:
        response.raise_for_status()
        with open(target_incomplete, "wb") as target_file:
            shutil.copyfileobj(response.raw, target_file)
        shutil.move(target_incomplete, target)
    print("Saved file {local_filename} ({local_filesize:d})".format(
            local_filename=target,
            local_filesize=os.path.getsize(target)))
    return target


def download_file(url, outdir, session):
    fname = _fname_from_url(url)
    target = os.path.join(outdir, fname)
    if not os.path.isfile(target):
        _download_file(url, target, session=session)
    return target


def stream_file(url, session):
    with session.get(url, stream=True) as response:
        response.raise_for_status()
        return response.content
