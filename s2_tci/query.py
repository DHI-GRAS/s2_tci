import logging

logger = logging.getLogger(__name__)

QUERY_KW = {
        'platformname': 'Sentinel-2',
        'producttype': 'S2MSI1C'}


def query_s2(api, area_geom, cloud_max=None, **query_kw):
    """Query SciHub for Sentinel 2 scenes

    Parameters
    ----------
    api : sentinelsat.SentinelAPI
        API instance
    area_geom : shapely.Polygon
        area of interest
    cloud_max : float, optional
        maximum percentage of cloud cover
        over entire image
    **query_kw : additional keyword arguments
        passed to api.query

    Returns
    -------
    OrderedDict
        results from query
    """
    area_wkt = area_geom.wkt
    kw = QUERY_KW.copy()
    if cloud_max is not None:
        kw['cloudcoverpercentage'] = (0, cloud_max)
    kw.update(query_kw)
    logger.debug(api.format_query(area=area_wkt, **kw))
    results = api.query(area=area_wkt, **kw)
    return results
