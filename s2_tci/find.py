import posixpath
import logging

from lxml import etree

logger = logging.getLogger(__name__)


def get_tci_download_url(product_url, product_title, s):
    """Get TCI download URL from tracing a product URL

    Parameters
    ----------
    product_url : str
        DHUS product URL
    product_title : str
        product title (the stuff before the .SAFE)
    s : requests.Session
        session

    Returns
    -------
    str
        download URL
    """
    granules_url = posixpath.join(
        product_url,
        'Nodes(\'{title}.SAFE\')'.format(title=product_title),
        'Nodes(\'GRANULE\')',
        'Nodes')
    logger.debug(granules_url)

    r = s.get(granules_url)
    granules_xml = r.text.encode('utf-8')

    tree = etree.fromstring(granules_xml)
    entries = tree.findall('.//{*}entry')
    e = entries[0]
    granule = e.find(".//{*}link[@title='Node']")
    gnode = granule.attrib['href']

    imgdata_url = posixpath.join(
        posixpath.dirname(granules_url),
        gnode,
        'Nodes(\'IMG_DATA\')',
        'Nodes')
    logger.debug(imgdata_url)

    r = s.get(imgdata_url)
    imgdata_xml = r.text.encode('utf-8')

    tree = etree.fromstring(imgdata_xml)
    links = tree.findall('.//{*}entry/{*}link[@type="application/octet-stream"]')
    hrefs = [l.attrib['href'] for l in links]
    tcinode_value = [h for h in hrefs if '_TCI.jp2' in h][0]

    tci_download_url = posixpath.join(
        posixpath.dirname(imgdata_url),
        tcinode_value)

    return tci_download_url


def tci_url_from_result(result, s):
    product_url = posixpath.dirname(result['link'])
    return get_tci_download_url(
        product_url=product_url,
        product_title=result['title'],
        s=s)


def get_tci_urls(result_dicts, s):
    """Get TCI URLs for sentinelsat query result dicts

    Parameters
    ----------
    result_dicts : iterable of dict
        result dictionaries
    s : requests.Session
        session

    Returns
    -------
    list of str
        TCI download URLs
    """
    urls = []
    for result in result_dicts:
        try:
            urls.append(tci_url_from_result(result, s))
        except Exception as e:
            logger.exception('Problem processing result.')
            continue
    return urls
