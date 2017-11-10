import posixpath
import logging

from lxml import etree

logger = logging.getLogger(__name__)


def get_tci_download_url(product_url, product_title, session):
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

    r = session.get(granules_url)
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

    r = session.get(imgdata_url)
    imgdata_xml = r.text.encode('utf-8')

    tree = etree.fromstring(imgdata_xml)
    links = tree.findall('.//{*}entry/{*}link[@type="application/octet-stream"]')

    tcinode_value = None
    for link in links:
        href = link.attrib['href']
        if '_TCI.jp2' in posixpath.basename(href):
            tcinode_value = href
            break

    if tcinode_value is None:
        raise RuntimeError('Could not find link to TCI file')

    tci_download_url = posixpath.join(
        posixpath.dirname(imgdata_url),
        tcinode_value)

    return tci_download_url


def tci_url_from_result(result, session):
    product_url = posixpath.dirname(result['link'])
    return get_tci_download_url(
        product_url=product_url,
        product_title=result['title'],
        session=session)


def get_tci_url(result_dict, session):
    """Get TCI URLs for sentinelsat query result dict

    Parameters
    ----------
    result_dict : dict
        result dictionary
    s : requests.Session
        session

    Returns
    -------
    str
        TCI download URL
    """
    try:
        return tci_url_from_result(result, session)
    except Exception as e:
        logger.exception('Problem processing result')
        return None
