from neomodel import db
from .geoname_mappings import get_geo_data
import logging
logger = logging.getLogger(__name__)

def update_geonames_locations_with_country_admin1():
    '''
        Add country code and admin1 (state/province) into node for easier querying
    '''
    query = """MATCH (n: Resource&GeoNamesLocation)
                WHERE n.countryCode IS NULL 
                AND n.countryList IS NULL
                AND n.featureClass IS NULL
                RETURN * LIMIT 1000"""
    while True:
        res, _ = db.cypher_query(query, resolve_objects=True)
        if len(res) == 0:
            logger.info("No more entities found, quitting")
            break
        for objs in res:
            obj = objs[0]
            geonamesid = obj.geoNamesId
            res = get_geo_data(geonamesid)
            if res is None:
                raise ValueError(f"No cached geo data for {geonamesid}")
            else:        
                obj.countryCode = res["country"]
                obj.admin1Code = res["admin1"]
                obj.featureCode = res["feature"]
                obj.countryList = res["country_list"]
                obj.save()


