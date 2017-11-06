import os
import re
import shutil


def _fname_from_url(url):
    try:
        return re.search('([\w_]*?_TCI.jp2)', url).group(1)
    except AttributeError:
        raise ValueError('Unable to get TCI file name from URL "{}".'.format(url))


def download_file(url, target, s):
    target_incomplete = target + '.incomplete'
    with s.get(url, stream=True) as response:
        response.raise_for_status()
        with open(target_incomplete, "wb") as target_file:
            shutil.copyfileobj(response.raw, target_file)
        shutil.move(target_incomplete, target)
    print("Saved file {local_filename} ({local_filesize:d})".format(
            local_filename=target,
            local_filesize=os.path.getsize(target)))
    return target


def download_urls(urls, outdir, s):
    targets = []
    for url in urls:
        fname = _fname_from_url(url)
        target = os.path.join(outdir, fname)
        if not os.path.isfile(target):
            download_file(url, target, s=s)
        targets.append(target)
    return targets
