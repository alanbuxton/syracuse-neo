from topics.services.typesense_service import TypesenseService
from topics.models import Organization, AboutUs, IndustrySectorUpdate, IndustryCluster, Resource
from django.conf import settings
from sentence_transformers import SentenceTransformer
from typing import Union, Tuple

class IndustryTypeSenseSearch(object):

    def __init__(self):
        self.ts = TypesenseService()
        self.collections = [
            Organization.typesense_collection,
            AboutUs.typesense_collection,
            IndustrySectorUpdate.typesense_collection,
            IndustryCluster.typesense_collection,
        ]
        self.model = SentenceTransformer(settings.EMBEDDINGS_MODEL)

    def uris_by_industry_text(self, text, regions: Union[set, list] = [], max_distance=0.18):
        res = self.do_query(text, regions)
        vals = self.matching_vals(res, max_distance, include_objects=False)
        return vals

    def objects_by_industry_text(self, text, regions: Union[set, list] = [], max_distance=0.18):
        res = self.do_query(text, regions)
        vals = self.matching_vals(res, max_distance)
        return vals
    
    def do_query(self, text, regions):
        query_vector = self.model.encode(text)
        res = self.ts.vector_search_multi(query_vector, self.collections, regions=regions)
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
                attribs = {"collection":row["collection_name"]}
                if doc_data.get("org_internal_ids"):
                    attribs["org_internal_ids"] = doc_data.get("org_internal_ids",[])
                uri_to_vector_distance[uri] = (vector_distance, attribs )
                uri_to_object[uri] = obj

        if include_objects is False:
            sorted_uris = sorted(uri_to_vector_distance.items(), key=lambda tup: (tup[1][0],tup[0]))
            return [(x,y[0],y[1]) for x,y in sorted_uris]
        objects = [(uri_to_object[x], v) for x, v in uri_to_vector_distance.items()]
        sorted_objects = sorted(objects, key=lambda tup: (tup[1][0],tup[0]))
        return [(x,y[0],y[1]) for x,y in sorted_objects]

def activities_by_industry_text_and_or_geo_typesense(industry_text: str, geo_codes: list[str], ts_search: Union[None,IndustryTypeSenseSearch]):
    geo_codes_plus_country_only = set(geo_codes)
    for geo_code in geo_codes:
        if "-" in geo_code:
            geo_codes_plus_country_only.add(geo_code[:2])

    if ts_search is None:
        ts_search = IndustryTypeSenseSearch()
    
    res = ts_search.objects_by_industry_text(industry_text, geo_codes_plus_country_only)


    
