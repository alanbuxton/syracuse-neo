from topics.services.typesense_service import TypesenseService
from topics.models import Organization, AboutUs, IndustrySectorUpdate, IndustryCluster, Resource
from django.conf import settings
from sentence_transformers import SentenceTransformer
from typing import Union, Tuple
from topics.activity_helpers import (get_activities_by_industry_geo_and_date_range,
                                     get_activities_by_org_uris_and_date_range,
                                     industry_sector_update_to_api_results)
import logging 
logger = logging.getLogger(__name__)

class IndustryGeoTypesenseSearch(object):

    def __init__(self):
        self.ts = TypesenseService()
        self.collections = [
            Organization.typesense_collection,
            AboutUs.typesense_collection,
            IndustrySectorUpdate.typesense_collection,
            IndustryCluster.typesense_collection,
        ]
        self.model = SentenceTransformer(settings.EMBEDDINGS_MODEL)

    def distance_based_on_words(self, text):
        if len(text.split()) > 1:
            max_distance = 0.22
        else:
            max_distance = 0.18
        return max_distance
    
    def query_and_filter(self, text, regions, max_distance, collections, include_objects):
        res = self.do_query(text, regions, collections=collections)
        max_distance = max_distance if max_distance else self.distance_based_on_words(text)
        vals = self.matching_vals(res, max_distance, include_objects=include_objects)
        return vals

    def uris_by_industry_text(self, text, regions: Union[set, list] = [], 
                              max_distance=None, collections=None):
        return self.query_and_filter(text, regions, max_distance, collections, include_objects=False)

    def objects_by_industry_text(self, text, regions: Union[set, list] = [], 
                                 max_distance=None, collections=None):
        return self.query_and_filter(text, regions, max_distance, collections, include_objects=True)
    
    def do_query(self, text, regions, collections=None):
        if collections is None:
            collections = self.collections
        query_vector = self.model.encode(text)
        res = self.ts.vector_search_multi(query_vector, collections, regions=regions)
        return res
    
    def do_query_by_collection(self, text, collection_name, regions=None):
        query_vector = self.model.encode(text)
        res = self.ts.vector_search_multi(query_vector, [collection_name], regions=regions)
        return res
    
    def matching_vals(self, res, max_distance, include_objects=True) -> list[ Tuple[ Union[Resource, str], float,dict]]:
        uri_to_vector_distance = {}
        uri_to_object = {}
        obj = None
        for row in res:
            vector_distance = row['vector_distance']
            if vector_distance > max_distance:
                continue
            doc_data = row['document']
            uri = doc_data['uri']
            if include_objects is True:
                obj = Resource.self_or_ultimate_target_node(uri)
                if obj is None:
                    raise ValueError(f"{uri} found in typesense, but not available in db")
                uri = obj.uri
            current_score = uri_to_vector_distance.get(uri,(9999,{}))[0]
            if vector_distance < current_score:
                collection = row["collection_name"]
                attribs = {"collection":collection} | doc_data
                uri_to_vector_distance[uri] = (vector_distance, attribs )
                uri_to_object[uri] = obj

        if include_objects is False:
            sorted_uris = sorted(uri_to_vector_distance.items(), key=lambda tup: (tup[1][0],tup[0]))
            return [(x,y[0],y[1]) for x,y in sorted_uris]
        objects = [(uri_to_object[x], v) for x, v in uri_to_vector_distance.items()]
        sorted_objects = sorted(objects, key=lambda tup: (tup[1][0],tup[0]))
        return [(x,y[0],y[1]) for x,y in sorted_objects]

def activities_by_industry_text_and_or_geo_typesense(industry_text: str, geo_codes: list[str], 
                                                     min_date, max_date, ts_search: Union[None,IndustryGeoTypesenseSearch] = None):
    geo_codes_plus_country_only = set(geo_codes)
    for geo_code in geo_codes:
        if "-" in geo_code:
            geo_codes_plus_country_only.add(geo_code[:2])

    if ts_search is None:
        ts_search = IndustryGeoTypesenseSearch()
    
    res = ts_search.uris_by_industry_text(industry_text, geo_codes_plus_country_only)

    all_activities = []

    relevant_org_uris = set()
    for uri, _, extra_data in res:
        collection = extra_data['collection']
        if collection == 'organizations':
            relevant_org_uris.add(uri)
        elif collection == 'industry_clusters':
            industry_id = extra_data["topic_id"]
            for geo_code in geo_codes:
                logger.info("Adding by industry for topic_id {industry_id} in {geo_code}")
                activities= get_activities_by_industry_geo_and_date_range(industry_id, geo_code, min_date, max_date) # Already all cached
                all_activities.append(activities)
        elif collection == 'industry_sector_updates':
            ind_sector_update = industry_sector_update_to_api_results(uri)
            all_activities.append(ind_sector_update)
        relevant_org_uris.update(extra_data.get('related_org_uris',[]))
    
    org_acts = get_activities_by_org_uris_and_date_range(relevant_org_uris,min_date, max_date)

    all_activities = all_activities + org_acts
    sorted_activities = sorted(all_activities, key=lambda x: x["date_published"], reverse=True)
    return sorted_activities


            


