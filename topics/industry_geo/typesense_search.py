from topics.services.typesense_service import TypesenseService
from topics.models import Organization, AboutUs, IndustrySectorUpdate, IndustryCluster, Resource
from django.conf import settings
from sentence_transformers import SentenceTransformer
from typing import Union, Tuple
from collections import Counter
from topics.activity_helpers import (get_activities_by_industry_geo_and_date_range,
                                     get_activities_by_org_uris_and_date_range,
                                     industry_sector_update_to_api_results)
import logging 
logger = logging.getLogger(__name__)

class IndustryGeoTypesenseSearch(object):

    def __init__(self):
        self.ts = TypesenseService()
        self.collections = {
            Organization.typesense_collection: {"one": 0.18, "more_than_one" :0.22} ,
            AboutUs.typesense_collection: {"narrow": 0.1, "broad": 0.18},
            IndustrySectorUpdate.typesense_collection: {"one":0.18, "more_than_one": 0.22},
            IndustryCluster.typesense_collection: {"one":0.18, "more_than_one": 0.18},
        }
        self.model = SentenceTransformer(settings.EMBEDDINGS_MODEL)

    def vector_distance_thresholds(self, text, collections_and_distances, min_scores):
        splitted = text.split()
        num_words = "more_than_one" if len(splitted) > 1 else "one"
        thresholds = {}
        for k,v in collections_and_distances.items():
            word_threshold = v.get(num_words)
            if word_threshold:
                thresholds[k] = word_threshold
            else:
                min_score = min_scores[k]
                if min_score < v["narrow"]:
                    thresholds[k] = v["narrow"]
                else:
                    thresholds[k] = v["broad"]
        return thresholds
    
    def query_and_filter(self, text, regions, collections_and_distances):
        res, min_scores = self.do_query(text, regions, collections=collections_and_distances.keys())
        max_distances = self.vector_distance_thresholds(text, collections_and_distances, min_scores)
        vals = self.matching_vals(res, max_distances)
        return vals

    def uris_by_industry_text(self, text, regions: Union[set, list] = [], 
                              collections_and_distances=None):
        if collections_and_distances is None:
            collections_and_distances = self.collections
        return self.query_and_filter(text, regions, collections_and_distances)
    
    def do_query(self, text, regions, collections=None):
        if collections is None:
            collections = self.collections.keys()
        query_vector = self.model.encode(text)
        res = self.ts.vector_search_multi(query_vector, collections, regions=regions)
        return res
    
    def do_query_by_collection(self, text, collection_name, regions=None):
        query_vector = self.model.encode(text)
        res = self.ts.vector_search_multi(query_vector, [collection_name], regions=regions)
        return res
    
    def matching_vals(self, res, max_distances) -> list[ Tuple[ Union[Resource, str], float,dict]]:
        uri_to_vector_distance = {}
        uri_to_object = {}
        obj = None
        for row in res:
            vector_distance = row['vector_distance']
            collection = row["collection_name"]
            doc_data = row['document']
            uri = doc_data['uri']
            if vector_distance > max_distances[collection]:
                logger.info(f"Dropping {uri} with score {vector_distance} vs {max_distances[collection]}")
                continue
            current_score = uri_to_vector_distance.get(uri,(9999,{}))[0]
            if vector_distance < current_score:
                collection = row["collection_name"]
                attribs = {"collection":collection} | doc_data
                uri_to_vector_distance[uri] = (vector_distance, attribs )
                uri_to_object[uri] = obj

        sorted_uris = sorted(uri_to_vector_distance.items(), key=lambda tup: (tup[1][0],tup[0]))
        return [(x,y[0],y[1]) for x,y in sorted_uris]
    
def get_top_industries_from_org_uris(org_uris, count=1) -> list:
    inds = []
    for uri in org_uris[:12]:
        xs = Resource.get_by_uri(uri).industryClusterPrimary
        inds.extend( [x.uri for x in xs])
    inds_c = Counter(inds)
    if inds_c == []:
        return []
    mc = [Resource.get_by_uri(x[0]).topicId for x in inds_c.most_common(count)]
    return mc


def activities_by_industry_text_and_or_geo_typesense(industry_text: str, geo_codes: list[str], 
                                                     min_date, max_date, ts_search: Union[None,IndustryGeoTypesenseSearch] = None):
    geo_codes_plus_country_only = set(geo_codes)
    for geo_code in geo_codes:
        if "-" in geo_code:
            geo_codes_plus_country_only.add(geo_code[:2])

    if ts_search is None:
        ts_search = IndustryGeoTypesenseSearch()

    res = ts_search.uris_by_industry_text(industry_text, geo_codes_plus_country_only)
    logger.info(f"uris by industry text: {res}")
    all_activities = []
    seen_uris = set()
    relevant_org_uris = set() 
    industry_ids = set()    
    org_uris = []
    related_org_uris = []
    for uri, _, extra_data in res:
        collection = extra_data['collection']
        if collection == 'organizations':
            org_uris.append(uri)
        elif collection == 'industry_clusters':
            industry_id = extra_data["topic_id"]
            industry_ids.add(industry_id) # Will collect industry data later
        elif collection == 'industry_sector_updates': 
            ent = industry_sector_update_to_api_results(uri)
            if ent['industry_sector_update_uri'] not in seen_uris:
                seen_uris.add(ent['industry_sector_update_uri'])
                all_activities.append(ent)
        related_org_uris.extend(extra_data.get('related_org_uris',[]))

    logger.info(f"Collected uris {len(org_uris)} org_uris, {len(related_org_uris)} related org uris")
    
    if len(industry_ids) == 0:
        industry_ids = get_top_industries_from_org_uris(org_uris,1)

    logger.info("Added most common industry")

    for industry_id in industry_ids:
        for geo_code in geo_codes:
            logger.info(f"Adding by industry for topic_id {industry_id} in {geo_code}")
            activities= get_activities_by_industry_geo_and_date_range(industry_id, geo_code, min_date, max_date) # Already all cached
            for act in activities:
                if act['activity_uri'] not in seen_uris:
                    seen_uris.add(act['activity_uri'])
                    all_activities.append(act)

    logger.info(f"Got industry activities: {len(all_activities)}")

    relevant_org_uris = set(org_uris + related_org_uris)
    
    org_acts = get_activities_by_org_uris_and_date_range(relevant_org_uris,min_date, max_date)

    logger.info("Got org_acts activities)")
    for act in org_acts:
        if act['activity_uri'] not in seen_uris:
            seen_uris.add(act['activity_uri'])
            all_activities.append(act)
    logger.info("combined all activities")
    sorted_activities = sorted(all_activities, key=lambda x: x["date_published"], reverse=True)
    return sorted_activities
