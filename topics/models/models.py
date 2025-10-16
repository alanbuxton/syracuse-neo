from neomodel import (StringProperty, StructuredNode,
    RelationshipTo, Relationship, RelationshipFrom, db, ArrayProperty,
    IntegerProperty, JSONProperty, StructuredRel)
from urllib.parse import urlparse
from syracuse.neomodel_utils import NativeDateTimeProperty
from collections import Counter
from neomodel.cardinality import OneOrMore, One, ZeroOrOne
from typing import Union, List
import cleanco
from django.conf import settings
from topics.util import geo_to_country_admin1
from integration.vector_search_utils import do_vector_search
from syracuse.cache_util import get_versionable_cache, set_versionable_cache
from syracuse.string_util import deduplicate_and_sort_by_frequency
from topics.industry_geo.industry_geo_cypher import industries_for_org, based_in_high_geo_names_locations_for_org
from topics.industry_geo.region_hierarchies import COUNTRIES_WITH_STATE_PROVINCE
import logging
from flags.state import flag_enabled 
import typesense

logger = logging.getLogger(__name__)

def uri_from_related(rel):
    if len(rel) == 0:
        return None
    return rel[0].uri

def longest(arr):
    if arr is None:
        return None
    elif isinstance(arr, str):
        return arr # Don't sort if it's just a string
    return sorted(arr,key=len)[-1]

def shortest(arr):
    if arr is None:
        return None
    elif isinstance(arr, str):
        return arr # Don't sort if it's just a string
    return sorted(arr,key=len)[0]

def print_friendly(vals, limit = 2):
    if vals is None:
        return None
    first_n = vals[:limit]
    val = ", ".join(first_n)
    extras = len(vals[limit:])
    if extras > 0:
        val = f"{val} and {extras} more"
    return val

def regions_from_geonames(geos, countries_with_admin1=COUNTRIES_WITH_STATE_PROVINCE):
    regions = set()
    for geo in geos:
        country_code = geo.countryCode
        regions.add(country_code)
        admin1 = geo.admin1Code
        if country_code in countries_with_admin1 and admin1 and admin1 != '00':
            regions.add( f"{country_code}-{admin1}")
    return regions

class WeightedRel(StructuredRel):
    weight = IntegerProperty(default=1)

class DocumentSourceRel(WeightedRel):
    documentExtract = StringProperty()

class Resource(StructuredNode):
    uri = StringProperty(unique_index=True, required=True)
    foundName = ArrayProperty(StringProperty())
    name = ArrayProperty(StringProperty())
    documentSource = RelationshipTo("Article","documentSource", model=DocumentSourceRel)
    internalDocId = IntegerProperty()
    internalId = IntegerProperty() # Unique ID for this entity, in case people need a unique Integer rather than URI (string)
    sameAsHigh = Relationship('Resource','sameAsHigh', model=WeightedRel)
    internalMergedSameAsHighToUri = StringProperty()

    @property
    def basedInHighGeoNamesLocation(self):
        return [] # override in sub-classes

    @property
    def pk(self):
        return self.internalId

    @property
    def connection_count(self):
        query = f"MATCH (n: Resource {{uri:'{self.uri}'}}) RETURN apoc.node.degree(n)"
        res, _ = db.cypher_query(query)
        return res[0][0]

    @property
    def sum_of_weights(self):
        cache_key = f"sum_weights_{self.uri}"
        weight = get_versionable_cache(cache_key)
        if weight:
            return weight
        query = f"""MATCH (n: Resource {{uri:'{self.uri}'}})-[x]-() RETURN sum(x.weight)"""
        res, _ = db.cypher_query(query)
        weight = res[0][0]
        set_versionable_cache(cache_key, weight)
        return weight
    
    @property
    def industry_clusters(self):
        return None # override in subclass
    
    @property
    def based_in_high_geonames_locations(self):
        return None # override in subclass
    
    @property
    def based_in_high_clean_names(self):
        return None # override in subclass

    @property
    def related_articles(self):
        cache_key = f"{self.uri}_related_articles"
        arts = get_versionable_cache(cache_key)
        if arts is not None:
            return arts
        if isinstance(self, Article) is True or isinstance(self, IndustryCluster) is True:
            arts = None
        else:
            arts = self.documentSource.all()
        set_versionable_cache(cache_key,arts)
        return arts
    
    def has_permitted_document_source(self,source_names):
        if isinstance(self, IndustryCluster):
            # Industry Clusters are always legitimate to show because they didn't come from specific articles
            return True 
        if hasattr(self, "sourceOrganization") and self.sourceOrganization in source_names:
            return True
        for x in self.related_articles or []:
            if x.sourceOrganization in source_names:
                return True
        return False
    
    def is_recent_enough(self, min_date):
        if isinstance(self, IndustryCluster):
            return True
        if isinstance(self, Article):
            if self.datePublished.date() >= min_date:
                return True
            else:       
                return False
        for x in self.related_articles or []:
            if x.datePublished.date() >= min_date:
                return True
        return False

    @staticmethod
    def get_by_uri(uri):
        assert isinstance(uri, str), f"Expected {uri} to be a string but it's a {type(uri)}"
        return Resource.nodes.get(uri=uri)

    @classmethod
    def unmerged_or_none_by_uri(cls, uri):
        return cls.unmerged_or_none({"uri":uri})

    @classmethod
    def unmerged_or_none(cls,params):
        assert isinstance(params, dict), f"Expected {params} to be a dict but it's a {type(params)}"
        return cls.nodes.filter(internalMergedSameAsHighToUri__isnull=True).get_or_none(**params)

    @property
    def industry_as_string(self):
        pass # override in relevant subclass

    @property
    def based_in_high_as_string(self):
        pass # override in relevant subclass - if in Mixin make sure Mixin is first https://stackoverflow.com/a/34090986/7414500

    @property
    def whereHighGeoName_as_str(self):
        pass # override in relevant subclass - if in Mixin make sure Mixin is first https://stackoverflow.com/a/34090986/7414500

    @property
    def all_relationships(self):
        '''
            In the case where something is not a neomodel relationship but we want it to appear in the all_directional_relationships
        '''
        return self.__all_relationships__
    
    @property
    def all_raw_relationships(self):
        '''
        Normally basedInHighGeonamesLocation and industryClusterPrimary will be filtered out based on weight when
        looking at all relationships. Except when merging new records.
        '''
        return self.all_relationships
    
    @property
    def dict_of_attribs(self):
        return self.__dict__
    
    @property
    def dict_of_raw_attribs(self):
        return self.dict_of_attribs

    @property
    def oldestDocumentSource(self):
        return self.documentSource.order_by("datePublished")[0]

    @property
    def oldestDatePublished(self):
        return self.oldestDocumentSource.datePublished

    @property
    def latestDatePublished(self):
        docs = self.documentSource.order_by("-datePublished")
        if len(docs) == 0:
            return None
        return docs[0].datePublished
    
    @staticmethod
    def self_or_ultimate_target_node(uri_or_object):
        if isinstance(uri_or_object, str):
            r = Resource.nodes.get_or_none(uri=uri_or_object)
        elif isinstance(uri_or_object, Resource):
            r = uri_or_object
        elif uri_or_object is None:
            return None
        else:
            raise ValueError(f"Don't know how to handle {uri_or_object}")
        if r is None:
            return None
        elif r.internalMergedSameAsHighToUri is None:
            return r
        else:
            return Resource.self_or_ultimate_target_node(r.internalMergedSameAsHighToUri)

    @staticmethod
    def self_or_ultimate_target_node_set(uris_or_objects: List):
        items = [Resource.self_or_ultimate_target_node(x) for x  in uris_or_objects]
        return list(set(items))

    @property
    def sourceDocumentURL(self):
        return uri_from_related(self.documentURL)

    @property
    def sourceName(self):
        docs = self.documentSource
        if docs is None or len(docs) == 0:
            return ''
        return self.documentSource[0].sourceOrganization

    def ensure_connection(self):
        if self.element_id_property is not None and db.database_version is None:
            # Sometimes seeing "_new_traversal() attempted on unsaved node" even though node is saved
            db.set_connection(settings.NEOMODEL_NEO4J_BOLT_URL)

    @property
    def best_name(self):
        # Should override in child classes if better way to come up with display name
        return self.longest_name

    @property
    def longest_name(self):
        return longest(self.name)

    @property
    def shortest_name(self):
        return shortest(self.name)

    def __hash__(self):
        return hash(self.uri)

    def __eq__(self, other):
        return self.uri == other.uri
    
    def __lt__(self, other):
        # highest degree first, then name
        self_degree, _ = db.cypher_query(f"MATCH (n: Resource) WHERE n.uri = '{self.uri}' RETURN apoc.node.degree(n)")
        other_degree, _ = db.cypher_query(f"MATCH (n: Resource) WHERE n.uri = '{other.uri}' RETURN apoc.node.degree(n)")
        self_degree = self_degree[0][0]
        other_degree = other_degree[0][0]
        if self_degree > other_degree:
            return True
        elif other_degree > self_degree:
            return False
        return self.best_name < other.best_name

    def serialize(self):
        self.ensure_connection()
        return {
            "entity_type": self.__class__.__name__,
            "label": self.best_name,
            "name": self.name,
            "uri": self.uri,
        }

    def serialize_no_none(self):
        vals = self.serialize()
        return {k:v for k,v in vals.items() if v is not None}

    def split_uri(self):
        parsed_uri = urlparse(self.uri)
        splitted_uri = parsed_uri.path.split("/") # first element is empty string because path starts with /
        assert len(splitted_uri) == 4, f"Expected {self.uri} path to have exactly 3 parts but it has {len(splitted_uri)}"
        return {
            "domain": parsed_uri.netloc,
            "path": splitted_uri[1],
            "doc_id": splitted_uri[2],
            "name": splitted_uri[3],
        }
    
    def delete_node_and_related(self):
        logger.info(f"delete_node_and_related {self.uri}")
        related_nodes_with_weights, _ = db.cypher_query(f"MATCH (n: Resource {{uri:'{self.uri}'}})-[rel]-(other: Resource) RETURN n.uri, type(rel), rel.weight, other.uri")
        extra_rels_to_delete = []
        for _, rel_type, weight, other_uri in related_nodes_with_weights:
            if rel_type != "sameAsHigh":
                extra_rels_to_delete.append( (rel_type, weight, other_uri))
        merged_nodes = Resource.nodes.filter(internalMergedSameAsHighToUri=self.uri)
        for m in merged_nodes:
            m.internalMergedSameAsHighToUri = None
            m.save()
        if self.internalMergedSameAsHighToUri is not None:
            next_node = Resource.get_by_uri(self.internalMergedSameAsHighToUri)
            next_node.recursively_remove_weighted_relationships(extra_rels_to_delete)
        self.delete()
        
    def recursively_remove_weighted_relationships(self, extra_rels_to_delete):
        logger.debug(f"recursively_remove_weighted_relationships for {self.uri} - {extra_rels_to_delete}")
        for rel_type, weight, other_uri in extra_rels_to_delete:
            if rel_type == 'sameAsHigh' or rel_type == 'sameAsNameOnly':
                continue
            core_query = f"MATCH (n: Resource {{uri:'{self.uri}'}})-[rel:{rel_type}]-(other: Resource {{uri:'{other_uri}'}})"
            existing_weight_rels,_ = db.cypher_query(f"{core_query} RETURN rel.weight")
            if len(existing_weight_rels) == 0: # No matching entity found
                continue
            assert (len(existing_weight_rels) == 1), f"Found {existing_weight_rels} for {self.uri}, {rel_type}, {weight}, {other_uri}. Query was {core_query}"
            existing_weight = existing_weight_rels[0][0]
            new_weight = existing_weight - weight
            if new_weight < 0:
                logger.warning(f"new weight is {new_weight} for {self.uri}, {rel_type}, {weight}, {other_uri}")
            if new_weight <= 0:
                delete_query = f"{core_query} DELETE rel"
            else:
                delete_query = f"{core_query} SET rel.weight = {new_weight}"
            logger.debug(delete_query)
            db.cypher_query(delete_query)
        if self.internalMergedSameAsHighToUri is not None:
            next_node = Resource.get_by_uri(self.internalMergedSameAsHighToUri)
            next_node.recursively_remove_weighted_relationships(extra_rels_to_delete)


    def all_directional_relationships(self, **kwargs):
        '''
            Returns dicts:
            from_uri
            label: relationship label
            direction:
            other_node:
            relationship_data: (only if source is Activity and other node is Article)
        '''
        override_from_uri = kwargs.get("override_from_uri")
        source_names = kwargs.get("source_names")
        min_date = kwargs.get("min_date")
        if override_from_uri is not None:
            from_uri = override_from_uri
        else:
            from_uri = self.uri
        all_rels = []
        logger.debug(f"Source node: {self.uri} - has permitted source with {source_names}? {self.has_permitted_document_source(source_names)}")
        for key, rel in self.all_relationships:
            if isinstance(rel, RelationshipTo):
                direction = "to"
            elif isinstance(rel, RelationshipFrom):
                direction = "from"
            elif isinstance(rel, List):
                direction = "to" # Only applies for now to industryClusterPrimary and basedInHighGeoNamesLocation
            else:
                continue

            for x in self.dict_of_attribs[key]:
                other_node = Resource.self_or_ultimate_target_node(x)
                if other_node.has_permitted_document_source(source_names) is False:
                    logger.debug(f"Skipping {other_node.uri} due to invalid source_names")
                    continue
                if min_date is not None and other_node.is_recent_enough(min_date) is False:
                    logger.debug(f"Skipping {other_node} as it is too old")
                    continue
                if isinstance(other_node, ActivityMixin) and other_node.internalMergedActivityWithSimilarRelationshipsToUri is not None:
                    logger.debug(f"Skipping {other_node.uri} because it was merged into {other_node.internalMergedActivityWithSimilarRelationshipsToUri}")
                    continue
                logger.debug(f"Working on {other_node.uri}")
                vals = {"from_uri": from_uri, "label":key, "direction":direction, "other_node":other_node}
                if isinstance(self, ActivityMixin) and isinstance(x, Article):
                    document_extract = self.documentSource.relationship(x).documentExtract
                    vals["document_extract"] = document_extract
                elif isinstance(self, Article) and isinstance(x, ActivityMixin):
                    document_extract = self.relatedEntity.relationship(x).documentExtract
                    vals["document_extract"] = document_extract
                if vals not in all_rels:
                    all_rels.append( vals )
        return all_rels
    

    @staticmethod
    def merge_node_connections(source_node, target_node, field_to_update="internalMergedSameAsHighToUri",
                               run_as_re_merge:bool =False,live_mode: bool=True,
                               use_these_weights={}):
        '''
            Copy/Merge relationships from source_node to target_node

            re-merge is needed if we've reloaded an existing node A that was already merged into node B
            This will copy any new relationships, but won't increment weight for existing ones

        '''
        logger.info(f"Merging {source_node.uri} into {target_node.uri}. Re-merge? {run_as_re_merge}")
        was_changed = False
        for rel_key,_ in source_node.all_raw_relationships:
            if rel_key.startswith("sameAs"):
                logger.debug(f"ignoring {rel_key}")
                continue
            for other_node in source_node.dict_of_attribs[rel_key]:
                if hasattr(source_node,f"internal_{rel_key}"):
                    logger.debug("internal key, skipping")
                    continue
                if hasattr(other_node, "internalMergedActivityWithSimilarRelationshipsToUri") and (other_node.internalMergedActivityWithSimilarRelationshipsToUri is not None):
                    logger.debug(f"{other_node.uri} was already merged, ignoring")
                    continue
                old_rel = source_node.dict_of_raw_attribs[rel_key].relationship(other_node)
                if not rel_key in target_node.dict_of_attribs:
                    logger.debug(f"source: {source_node.uri} target {target_node.uri} rel {rel_key} doesnt exist while trying to connect {other_node.uri}" )
                    continue
                logger.debug(f"connecting {other_node.uri} to {target_node.uri} via {rel_key} from {source_node.uri}")                    
                already_connected = target_node.dict_of_attribs[rel_key].relationship(other_node)
                if already_connected is not None:
                    logger.debug(f"{target_node.uri} is already connected to {other_node.uri} via {rel_key}")
                    if run_as_re_merge is False: # if re-merging don't update existing nodes. This does mean we could have some nodes with lower weight than usual, but I'm assuming it's not material
                        was_changed = True
                        weight_delta = use_these_weights.get(other_node.uri, old_rel.weight)
                        already_connected.weight += weight_delta
                        if live_mode is True:
                            already_connected.save()
                else:
                    logger.debug(f"{source_node.uri} -> {target_node.uri}: {rel_key} {other_node.uri}")
                    new_rel = target_node.dict_of_attribs[rel_key].connect(other_node)
                    if hasattr(old_rel, 'documentExtract'):
                        new_rel.documentExtract = old_rel.documentExtract
                    new_rel.weight = old_rel.weight
                    was_changed = True
                    if live_mode is True:
                        new_rel.save()
        if was_changed is True and live_mode is True: 
            logger.debug(f"LIVE MODE - SAVING CHANGES")
            target_node.save()
            if run_as_re_merge is False:
                if field_to_update == "internalMergedSameAsHighToUri":
                    source_node.internalMergedSameAsHighToUri = target_node.uri
                elif field_to_update == "internalMergedActivityWithSimilarRelationshipsToUri":
                    source_node.internalMergedActivityWithSimilarRelationshipsToUri = target_node.uri
                else:
                    raise ValueError(f"Merging but have not selected a valid field to update {field_to_update}")
                source_node.save()
        return was_changed
   
    def to_typesense_doc(self) -> Union[list,dict]:
        return {}
    
    typesense_collection = ""
    
    def index_in_typesense(self):
        use_typesense = settings.INDEX_IN_TYPESENSE_ON_SAVE
        if use_typesense is False:
            return
        client = typesense.Client(settings.TYPESENSE_CONFIG)
        documents = self.to_typesense_doc()
        try:
            if isinstance(documents, dict):
                # Single document upsert
                return client.collections[self.typesense_collection].documents.upsert(documents)
            elif isinstance(documents, list):
                # Bulk upsert
                return client.collections[self.typesense_collection].documents.import_(
                    documents,
                    {"action": "upsert"}
                )
            else:
                raise ValueError("to_typesense_doc() must return a dict or list of dicts")
        except Exception as e:
            logger.error(f"Error indexing into Typesense: {e}")
            return None

    def save(self):
        super().save()
        if self.typesense_collection:
            self.index_in_typesense()
        return self


class Article(Resource):
    headline = StringProperty(required=True)
    url = Relationship("Resource","url", cardinality=One, model=WeightedRel)
    sourceOrganization = StringProperty(required=True)
    datePublished = NativeDateTimeProperty(required=True)
    dateRetrieved = NativeDateTimeProperty(required=True)
    relatedEntity = RelationshipFrom("Resource","documentSource",
            model=DocumentSourceRel, cardinality=OneOrMore)
    
    @staticmethod
    def available_source_names_dict():
        cache_key = "available_source_names"
        sources = get_versionable_cache(cache_key)
        if sources is not None:
            return sources
        res = db.cypher_query("MATCH (n: Resource:Article) RETURN DISTINCT(n.sourceOrganization)")
        sources = {x[0].lower():x[0] for x in res[0]}
        assert None not in sources, f"We have an Article with None sourceOrganization"
        set_versionable_cache(cache_key,sources)
        return sources
    
    @staticmethod
    def all_sources():
        return sorted(list(Article.available_source_names_dict().values()))
    
    @staticmethod
    def sources_with_more_than_n_articles(limit=10):
        cache_key=f"sources_with_more_than_{limit}"
        res = get_versionable_cache(cache_key)
        if res:
            return res
        query = f"""MATCH (a: Article)
                    WITH a.sourceOrganization AS source_org, COUNT(a.sourceOrganization) AS count_of_org
                    WHERE count_of_org >= {limit}
                    RETURN source_org, count_of_org ORDER BY count_of_org DESC"""
        vals, _ = db.cypher_query(query)
        return {k: v for k,v in vals}

    @staticmethod
    def core_sources():
        '''
            Generic, high quality sources that we get good results with.
            Will run with newer data sources for a while before adding them to this list.
        '''
        return [
            'Associated Press',
            'Business Insider',
            'Business Wire',
            'CNN',
            'CNN International',
            'GlobeNewswire',
            'Indiatimes',
            'PR Newswire',
            'PR Newswire UK',
            'PR Web',
            'Reuters',
            'Sifted',
            'South China Morning Post',
            'TechCrunch',
            'The Globe and Mail',
            'prweb',
        ]
    
    @property
    def is_core(self):
        return self.sourceOrganization in Article.core_sources()

    @property
    def archive_date(self):
        closest_date = self.dateRetrieved or self.datePublished
        return closest_date.strftime("%Y%m%d")

    @property
    def archiveOrgPageURL(self):
        return f"https://web.archive.org/{self.archive_date}235959/{self.documentURL}"

    @property
    def archiveOrgListURL(self):
        return f"https://web.archive.org/{self.archive_date}*/{self.documentURL}"

    @property
    def best_name(self):
        return self.headline

    @property
    def documentURL(self):
        return self.url[0].uri

    def serialize(self):
        vals = super().serialize()
        vals['headline'] = self.best_name
        vals['document_url'] = self.documentURL
        vals['source_organization'] = self.sourceOrganization
        vals['date_published'] = self.datePublished
        vals['date_retrieved'] = self.dateRetrieved
        vals['internet_archive_page_url'] = self.archiveOrgPageURL
        vals['internet_archive_list_url'] = self.archiveOrgListURL
        return vals


class IndustryCluster(Resource):
    representation = ArrayProperty(StringProperty())
    representativeDoc = ArrayProperty(StringProperty())
    childRight = RelationshipTo("IndustryCluster","childRight", cardinality=ZeroOrOne)
    childLeft = RelationshipTo("IndustryCluster","childLeft", cardinality=ZeroOrOne)
    parentLeft = RelationshipFrom("IndustryCluster","childLeft", cardinality=ZeroOrOne)
    parentRight = RelationshipFrom("IndustryCluster","childRight", cardinality=ZeroOrOne) 
    topicId = IntegerProperty()
    uniqueName = StringProperty()
    orgsPrimary = RelationshipFrom("Organization","industryClusterPrimary", model=WeightedRel)
    orgsSecondary = RelationshipFrom("Organization","industryClusterSecondary", model=WeightedRel)
    peoplePrimary = RelationshipFrom("Person","industryClusterPrimary", model=WeightedRel)
    peopleSecondary = RelationshipFrom("Person","industryClusterSecondary", model=WeightedRel)
    representative_doc_embedding_json = JSONProperty()

    @property
    def pk(self):
        return self.topicId
    
    @staticmethod
    def get_by_industry_id(industry_id):
        if industry_id is None:
            return None
        try:
            industry_id = int(industry_id)
        except ValueError:
            logger.warning(f"Couldn't convert {industry_id} to int")
            return None
        return IndustryCluster.nodes.get_or_none(topicId=industry_id)

    @property
    def mergedOrgsPrimary(self):
        cache_key = f"{self.topicId}_orgs"
        res = get_versionable_cache(cache_key)
        if res is not None:
            return res
        res = []
        for x in self.orgsPrimary:
            res.append(Organization.self_or_ultimate_target_node(x))
        set_versionable_cache(cache_key, res)
        return res

    @property
    def topic_id(self):
        return self.topicId
    
    @property
    def unique_name(self):
        return self.uniqueName
    
    @property
    def representative_docs(self):
        return self.representativeDoc

    def serialize(self):
        vals = super(IndustryCluster, self).serialize()
        if self.representativeDoc is None:
            name_words = self.uniqueName.split("_")[1:]
            name_words = ", ".join(name_words)
        else:
            name_words = sorted(self.representativeDoc, key=len)[-1]
        vals['label'] = name_words
        vals['representative_docs'] = (", ").join(self.representativeDoc) if self.representativeDoc else None
        vals['internal_cluster_id'] = self.topicId
        vals['unique_name'] = self.uniqueName
        return vals

    @property
    def best_name(self):
        return self.longest_representative_doc

    @property
    def longest_representative_doc(self):
        docs = self.representativeDoc
        if docs is None:
            return None
        val = sorted(docs,key=len)[-1]
        mappings = {"ott software": "OTT Software"}
        val = mappings.get(val.lower(),val)
        return val

    @staticmethod
    def for_external_api():
        vals = []
        for ind in IndustryCluster.nodes.filter(representativeDoc__isnull=False):
            serialized = {"industry_id": ind.topicId,
                          "representative_docs_list": ind.representativeDoc,
                          "longest_representative_doc": ind.longest_representative_doc,
            }
            vals.append(serialized)
        return vals

    @staticmethod
    def representative_docs_to_industry():
        docs_to_industry_uri = []
        for ind in IndustryCluster.nodes.filter(representativeDoc__isnull=False):
            if ind.topicId == -1:
                continue
            longest_doc = sorted(ind.representativeDoc,key=len)[-1]
            docs_to_industry_uri.append( (ind.topicId, longest_doc))
        return docs_to_industry_uri
    
    @staticmethod
    def leaf_keywords(ignore_cache=False):
        cache_key = "industry_leaf_keywords"
        res = get_versionable_cache(cache_key)
        if res is not None and ignore_cache is False:
            return res
        keywords = {}
        for node in IndustryCluster.used_leaf_nodes_only():
            words = node.uniqueName.split("_")
            idx = words[0]
            for word in words[1:]:
                if word not in keywords.keys():
                    keywords[word] = set()
                keywords[word].add(idx)
        set_versionable_cache(cache_key, keywords)
        return keywords

    @staticmethod
    def top_node():
        query = "MATCH (n: IndustryCluster) WHERE NOT (n)<-[:childLeft|childRight]-(:IndustryCluster) AND n.topicId <> -1 return n"
        results, _ = db.cypher_query(query,resolve_objects=True)
        return results[0][0]

    @staticmethod
    def used_leaf_nodes_only():
        # Only used leaf nodes
        query = "MATCH (n: IndustryCluster) WHERE NOT (n)-[:childLeft|childRight]->(:IndustryCluster) AND (n)-[:industryClusterPrimary]-() RETURN n"
        results, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in results for x in sublist]
        return flattened
    
    @staticmethod
    def all_leaf_nodes():
        query = "MATCH (n: IndustryCluster) WHERE NOT (n)-[:childLeft|childRight]->(:IndustryCluster) AND n.topicId <> -1 return n"
        results, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in results for x in sublist]
        return flattened

    @staticmethod
    def parents_of(nodes):
        parent_objects = []
        for node in nodes:
            for p in node.parents:
                if p not in nodes:
                    parent_objects.append(p)
        return parent_objects

    @staticmethod
    def first_parents():
        return IndustryCluster.parents_of(IndustryCluster.used_leaf_nodes_only())

    @staticmethod
    def second_parents():
        return IndustryCluster.parents_of(IndustryCluster.first_parents())

    @staticmethod
    def with_descendants(topic_id,max_depth=500):
        if topic_id is None:
            return None
        root = IndustryCluster.nodes.get_or_none(topicId=topic_id)
        if root is None:
            return []
        descendant_uris,_ = db.cypher_query(f"MATCH (n: IndustryCluster {{uri:'{root.uri}'}})-[x:childLeft|childRight*..{max_depth}]->(o: IndustryCluster) return o.uri;")
        flattened = [x for sublist in descendant_uris for x in sublist]
        return [root.uri] + flattened

    @property
    def friendly_name_and_id(self):
        vals = self.uniqueName.split("_")
        name = ", ".join(vals[1:]).title()
        return name, vals[0]
    
    @staticmethod
    def by_name(name, limit=10, request=None):
        if request and flag_enabled("FEATURE_TYPESENSE", request=request):
            logger.warning("Using FEATURE_TYPESENSE - needs reimplementation")
        else:
            return IndustryCluster.by_representative_doc_words(name, limit=limit)

    @staticmethod
    def by_representative_doc_words(name, limit=10, min_score=0.85):
        query = f''' CALL db.index.vector.queryNodes('industry_cluster_representative_docs_vec', 500, $query_embedding)
        YIELD node, score
        WITH node, score
        WHERE score >= {min_score}
        RETURN node
        ORDER BY score DESCENDING
        '''
        name = name.lower()
        res = do_vector_search(name, query)
        flattened = [x[0] for x in res]
        return flattened[:limit]
    
    # @staticmethod
    # def by_embeddings_typesense(name, limit=10):
    #     hits = do_vector_search_typesense(name, IndustryCluster.typesense_collection, limit=limit)
    #     # returned_vals may have multiple matches per industry - if so then the entry with most matches wins
    #     topic_ids = [x['document']['topic_id'] for x in hits]
    #     c = Counter(topic_ids).most_common()
    #     ordered_topic_ids = [x[0] for x in c]
    #     query = f"""MATCH (n: Resource&IndustryCluster) WHERE n.topicId IN {ordered_topic_ids}
    #                 RETURN n
    #                 ORDER BY apoc.coll.indexOf({ordered_topic_ids}, n.topicId)"""
    #     vals, _ = db.cypher_query(query, resolve_objects=True)
    #     flattened = [x[0] for x in vals]
    #     return flattened[:limit]

    @staticmethod
    def embedding_field_name():
        return ""
    
    def to_typesense_doc(self):
        docs = []
        # Representative Docs
        rep_embeddings = self.representative_doc_embedding_json or []
        for idx, embedding in enumerate(rep_embeddings):
            docs.append({
                "id": f"{self.internalId}_rep_{idx}",   # unique per embedding
                "topic_id": self.topicId,
                "uri": self.uri,
                "embedding": embedding,
            })
        return docs

    typesense_collection = "industry_clusters" 
    
    @classmethod
    def typesense_schema(cls):
        schema = {
            'name': cls.typesense_collection,
            'fields': [
                {'name': 'topic_id', 'type': 'int32'},
                {'name': 'embedding', 'type': 'float[]', 'num_dim': 768},
            ],
            'default_sorting_field': 'topic_id',
        }
        return schema


class ActivityMixin:
    activityType = ArrayProperty(StringProperty())
    when = ArrayProperty(NativeDateTimeProperty())
    whenRaw = ArrayProperty(StringProperty())
    status = ArrayProperty(StringProperty())
    whereHighRaw = ArrayProperty(StringProperty())
    whereHighClean = ArrayProperty(StringProperty())
    whereHighGeoNamesLocation = RelationshipTo('GeoNamesLocation','whereHighGeoNamesLocation', model=WeightedRel)
    internalMergedActivityWithSimilarRelationshipsToUri = StringProperty() # Merging equivalent activities (can be subset or same rels)

    @property
    def whereHighGeoName_as_str(self):
        cache_key = f"{self.__class__.__name__}_activity_mixin_{self.uri}"
        names = get_versionable_cache(cache_key)
        if names is not None:
            return names
        names = []
        for x in self.whereHighGeoNamesLocation:
            if x.name is None:
                continue
            names.extend(x.name)
        name = print_friendly(names)
        set_versionable_cache(cache_key,name)
        return name

    @property
    def longest_activityType(self):
        return longest(self.activityType)

    @property
    def status_as_string(self):
        if self.status is None or len(self.status) > 1:
            return "unknown"
        elif self.status[0] == "has not happened":
            return "unknown"
        else:
            return self.status[0]

    @property
    def activity_fields(self):
        if self.activityType is None:
            activityType_title = "Activity"
        else:
            activityType_title = self.longest_activityType.title()
        return {
            "activity_type": activityType_title,
            "label": f"{activityType_title} ({self.sourceName}: {self.oldestDatePublished.strftime('%b %Y')})",
            "status": self.status,
            "when": self.when,
            "when_raw": self.whenRaw,
            "where_high_raw": self.whereHighRaw,
            "where_high_clean": self.whereHighClean,
        }

    @property
    def whereHighGeoNameRDFURL(self):
        return [x.uri for x in self.whereHighGeoNameRDF]

    @property
    def whereHighGeoNameURL(self):
        uris = []
        for x in self.whereHighGeoNameRDF:
            uri = x.uri.replace("/about.rdf","")
            uris.append(uri)
        return uris
    
    def uniquify(self,d):
        return {
            k: Resource.self_or_ultimate_target_node_set(vs) for k,vs in d.items()
        }


class GeoNamesLocation(Resource):
    geoNamesId = IntegerProperty()
    geoNames = Relationship('Resource','geoNamesURL')
    countryCode = StringProperty() # added after import
    admin1Code = StringProperty() # added after import
    countryList = ArrayProperty(StringProperty()) # added after import
    featureCode = StringProperty() # added after import
    basedInHighGeoNamesLocation = RelationshipFrom('Resource','basedInHighGeoNamesLocation',model=WeightedRel)
    whereHighGeoNamesLocation = RelationshipFrom('Resource','whereHighGeoNamesLocation',model=WeightedRel)

    @property
    def best_name(self):
        loc_name = longest(self.name)
        admin1_str = ""
        if self.admin1Code and self.admin1Code != '00' and self.countryCode in COUNTRIES_WITH_STATE_PROVINCE:
            admin1_str = f"-{self.admin1Code}"
        name = f"{loc_name} ({self.countryCode}{admin1_str})"
        return name

    @property
    def pk(self):
        return self.geoNamesId
    
    @property
    def geoNamesURL(self):
        return uri_from_related(self.geoNames)

    @property
    def geoNamesRDFURL(self):
        if self.geoNamesURL is None:
            return None
        uri = self.geoNamesURL + "/about.rdf"
        return uri

    def serialize(self):
        vals = super().serialize()
        vals['geonames_id'] = self.geoNamesId
        vals['geonames_rdf_url'] = self.geoNamesRDFURL
        vals['geonames_url'] = self.geoNamesURL
        return vals
    
    @staticmethod
    def uris_by_geo_code(geo_code):
        if geo_code is None or geo_code.strip() == '':
            return None
        cache_key = f"geonames_locations_{geo_code}"
        res = get_versionable_cache(cache_key)
        if res is not None:
            return res
        country_code, admin1_code = geo_to_country_admin1(geo_code)
        query = f"""MATCH (n: GeoNamesLocation) WHERE n.countryCode = '{country_code}' """
        if admin1_code is not None:
            query = f"{query} AND n.admin1Code = '{admin1_code}'"
        query = f"{query} RETURN n.uri"
        vals, _ = db.cypher_query(query,resolve_objects=True)
        uris = [x[0] for x in vals]
        set_versionable_cache(cache_key, uris)
        return uris

class Organization(Resource):
    description = ArrayProperty(StringProperty())
    industry = ArrayProperty(StringProperty())
    investor = RelationshipTo('CorporateFinanceActivity','investor', model=WeightedRel)
    buyer =  RelationshipTo('CorporateFinanceActivity', 'buyer', model=WeightedRel)
    protagonist = RelationshipTo('CorporateFinanceActivity', 'protagonist', model=WeightedRel)
    participant = RelationshipTo('CorporateFinanceActivity', 'participant', model=WeightedRel)
    vendor = RelationshipFrom('CorporateFinanceActivity', 'vendor', model=WeightedRel)
    target = RelationshipFrom('CorporateFinanceActivity', 'target', model=WeightedRel)
    hasRole = RelationshipTo('Role','hasRole', model=WeightedRel)
    locationAdded = RelationshipTo('LocationActivity','locationAdded', model=WeightedRel)
    locationRemoved = RelationshipTo('LocationActivity','locationRemoved', model=WeightedRel)
    internal_industryClusterPrimary = RelationshipTo('IndustryCluster','industryClusterPrimary', model=WeightedRel)
    internal_basedInHighGeoNamesLocation = RelationshipTo('GeoNamesLocation','basedInHighGeoNamesLocation', model=WeightedRel)
    partnership = RelationshipTo('PartnershipActivity','partnership', model=WeightedRel)
    awarded = RelationshipTo('PartnershipActivity','awarded', model=WeightedRel)
    providedBy = RelationshipFrom('PartnershipActivity','providedBy', model=WeightedRel)
    basedInHighRaw = ArrayProperty(StringProperty())
    basedInHighClean = ArrayProperty(StringProperty())
    productOrganization = RelationshipTo('ProductActivity','productActivity', model=WeightedRel)
    aboutUs = RelationshipTo('AboutUs','hasAboutUs', model=WeightedRel)
    analystRating = RelationshipTo('AnalystRatingActivity', 'hasAnalystRating', model=WeightedRel)
    equityAction = RelationshipTo('EquityActionsActivity','hasEquityActionsActivity', model=WeightedRel)
    finacialReporting = RelationshipTo('FinancialReportingActivity','hasFinancialReportingActivity',model=WeightedRel)
    financials = RelationshipTo('FinancialsActivity','hasFinancialsActivity', model=WeightedRel)
    incident = RelationshipTo('IncidentActivity','hasIncidentActivity', model=WeightedRel)
    marketing = RelationshipTo('MarketingActivity','hasMarketingActivity', model=WeightedRel)
    operations = RelationshipTo('OperationsActivity','hasOperationsActivity', model=WeightedRel)
    recognition = RelationshipTo('RecognitionActivity','hasRecognitionActivity', model=WeightedRel)
    regulatory = RelationshipTo('RegulatoryActivity','hasRegulatoryActivity', model=WeightedRel)
    internalCleanName = ArrayProperty(StringProperty())
    internalCleanShortName = ArrayProperty(StringProperty())
    mentionedIn = RelationshipTo('IndustrySectorUpdate', 'mentionedIn', model=WeightedRel)
    top_industry_names_embedding_json = JSONProperty()

    @property
    def all_relationships(self):
        tmp_rels = tuple([(label,rel) for label,rel in self.__all_relationships__ if label.startswith("internal_") is False])
        return tmp_rels + (
            ("industryClusterPrimary", self.industryClusterPrimary),
            ("basedInHighGeoNamesLocation", self.basedInHighGeoNamesLocation)
        )

    @property
    def all_raw_relationships(self):
        tmp_rels = tuple([(label,rel) for label,rel in self.__all_relationships__ ])
        return tmp_rels
    
    @property
    def dict_of_raw_attribs(self):
        vals = self.__dict__.copy()
        vals["industryClusterPrimary"] = self.internal_industryClusterPrimary
        vals["basedInHighGeoNamesLocation"] = self.internal_basedInHighGeoNamesLocation
        return vals

    @property
    def dict_of_attribs(self):
        vals = self.__dict__.copy()
        vals["industryClusterPrimary"] = self.industryClusterPrimary
        vals["basedInHighGeoNamesLocation"] = self.basedInHighGeoNamesLocation
        return vals

    @property
    def basedInHighGeoNamesLocation(self):
        return based_in_high_geo_names_locations_for_org(self.uri)

    @property
    def industryClusterPrimary(self):
        return industries_for_org(self.uri)

    @property
    def industry_clusters(self):
        return self.industryClusterPrimary

    @property
    def based_in_high_geonames_locations(self):
        return self.basedInHighGeoNamesLocation

    @property
    def based_in_high_as_string(self):
        all_loc_names = [x.name for x in (self.basedInHighGeoNamesLocation or [])]
        flattened = [x for sublist in all_loc_names for x in sublist]
        if len(flattened) == 0:
            return None
        c = Counter(flattened)
        ordered = [x[0] for x in c.most_common()]
        return "; ".join(ordered)
    
    @property
    def based_in_high_clean_names(self):
        vals = set(self.basedInHighClean) if self.basedInHighClean is not None else set()
        for x in self.sameAsHigh:
            if not hasattr(x, "basedInHighClean"):
                logger.warning(f"{x.uri} is same_as_high for {self.uri} but is not an Organization, it's a {x.__class__}")
                continue
            if x.basedInHighClean is None:
                continue
            vals.update(x.basedInHighClean)
        return list(vals)
    
    @property
    def industry_list(self):
        cache_key = f"industry_list_{self.uri}"
        res = get_versionable_cache(cache_key)
        if res is not None:
            return res
        inds = self.industryClusterPrimary
        set_versionable_cache(cache_key,inds)
        return inds

    @property
    def top_industry(self):
        inds = self.industry_list
        if inds:
            return inds[0]
        else:
            return None

    @property
    def industry_as_string(self):
        inds = self.industry_list
        if not inds:
            return None
        sorted_inds = sorted([x.longest_representative_doc for x in inds])
        top_inds = sorted_inds[:2]
        top_inds_as_str = ", ".join(sorted(top_inds))
        if len(sorted_inds) < 3:
            return top_inds_as_str
        other_ind_num = len(sorted_inds) - 2
        return f"{top_inds_as_str} and {other_ind_num} more"

    @property
    def best_name(self):
        cache_key = f"org_name_{self.uri}"
        name = get_versionable_cache(cache_key)
        if name is not None:
            return name
        name_cands = (self.name or []) + [] # Ensure name_cands is a copy
        for x in self.sameAsHigh:
            if x.name is not None:
                name_cands.extend(x.name)
        name_counter = Counter(name_cands)
        top_name = name_counter.most_common(1)[0][0]
        set_versionable_cache(cache_key, top_name)
        return top_name

    @staticmethod
    def get_best_name_by_uri(uri):
        org = Organization.self_or_ultimate_target_node(uri)
        if org is not None:
            return org.best_name

    @staticmethod
    def by_uris(uris):
        return Organization.nodes.filter(uri__in=uris)

    @property
    def shortest_name_length(self):
        words = shortest(self.name)
        if words is None:
            return 0
        return len(cleanco.basename(words).split())

    def serialize(self):
        vals = super().serialize()
        locs = [x.best_name for x in self.basedInHighGeoNamesLocation]
        org_vals = {"best_name": self.best_name,
                    "description": self.description,
                    "industry": self.industry_as_string,
                    "based_in_high_raw": self.basedInHighRaw,
                    "based_in_high_clean": self.basedInHighClean,
                    "line_of_business": "; ".join(self.top_industry_names(with_caps=True)),
                    "locations": "; ".join(locs),
                    "about_us": "; ".join(self.top_about_us(with_caps=True)),
                    }
        return {**vals,**org_vals}
    
    def get_role_activities(self,source_names=None):
        if source_names is None:
            source_names = Article.core_sources()
        role_activities = []
        for role in self.hasRole:
            acts = role.withRole.all()
            role_activities.extend([(role,act) for act in acts if act.has_permitted_document_source(source_names)])
        return role_activities

    @staticmethod
    def by_industry_text(name,limit=0.87):
        cache_key = f"org_by_industry_text_{name}_{limit}"
        res = get_versionable_cache(cache_key)
        if res is not None:
            return res
        query = f''' CALL db.index.vector.queryNodes('organization_industries_vec', 500, $query_embedding)
        YIELD node, score
        WITH node, score
        WHERE score >= {limit}
        RETURN node.uri, apoc.node.degree(node)
        ORDER BY score DESCENDING
        '''
        vals = do_vector_search(name, query)
        res = set()
        for val in vals:
            res.add( Organization.self_or_ultimate_target_node(val[0]).uri)
        set_versionable_cache(cache_key, res)
        return list(res)

    @staticmethod
    def by_industry_text_and_geo(name,country_code,admin1_code=None,limit=0.85):
        cache_key = f"org_by_industry_text_geo_{name}_{country_code}_{admin1_code}_{limit}"
        res = get_versionable_cache(cache_key)
        if res is not None:
            return res
        query = f'''CALL db.index.vector.queryNodes('organization_industries_vec', 500, $query_embedding) 
                    YIELD node, score
                    WITH node, score
                    WHERE score >= {limit}
                    WITH node, score
                    MATCH (node)-[:basedInHighGeoNamesLocation]-(r: Resource&GeoNamesLocation)
                    WHERE r.countryCode = '{country_code}'
                    '''
        if admin1_code is not None:
            query = f"{query} AND r.admin1Code = '{admin1_code}'"
        query = f"{query} RETURN node.uri, apoc.node.degree(node) ORDER BY score DESCENDING"
        vals = do_vector_search(name, query)
        res = [tuple(row) for row in vals]
        set_versionable_cache(cache_key, res)
        return list(res)
    
    def industry_names_from_merged_nodes(self, industry_names):
        if self.industry:
            industry_names.extend(self.industry)
            logger.debug(f"Adding industry names {self.industry} from {self.uri}")

        merged_nodes = Resource.nodes.filter(internalMergedSameAsHighToUri=self.uri)
        for node in merged_nodes:
            industry_names = node.industry_names_from_merged_nodes(industry_names)

        return industry_names
    
    def top_industry_names(self,with_caps=False):
        cache_key = f"top_ind_names_{self.uri}_{with_caps}"
        res = get_versionable_cache(cache_key)
        if res:
            return res
        inds = self.industry_names_from_merged_nodes([])
        if with_caps is True:
            over1 = deduplicate_and_sort_by_frequency(inds, min_count=2)
        else:
            inds_c = Counter( [x.lower() for x in inds] )
            over1 = [x for x,y in inds_c.items() if y > 1]
        set_versionable_cache(cache_key, over1)
        return over1
    
    def top_about_us(self,with_caps=False):
        cache_key = f"top_about_us_{self.uri}_{with_caps}"
        res = get_versionable_cache(cache_key)
        if res:
            return res
        abouts = [x.best_name for x in self.aboutUs]
        if with_caps is True:
            over1 = deduplicate_and_sort_by_frequency(abouts, min_count=2)
        else:
            c = Counter( [x.lower() for x in abouts] )
            over1 = [x for x,y in c.items() if y > 1]
        set_versionable_cache(cache_key, over1)
        return over1

    @property
    def regions(self):
        return list(regions_from_geonames(self.basedInHighGeoNamesLocation))

    def to_typesense_doc(self):
        '''
        Collect industry descriptions from any orgs merged into me
        '''
        docs = []
        jsons = self.top_industry_names_embedding_json or []
        names = self.name or []
        if len(names) == 0:
            name0 = None
            names1plus = []
        else:
            name0 = names[0]
            names1plus = names[1:]
        regions = self.regions
        for idx, embedding in enumerate(jsons):
            docs.append({
                'id': f"{self.internalId}_industry_{idx}",
                'internal_id': self.internalId,
                'uri': self.uri,
                'name': name0,
                'region_list': regions,
                'embedding': embedding,
            })
        for name in names1plus:
            docs.append({
                    'id': f"{self.internalId}_names",
                    'internal_id': self.internalId,
                    'uri': self.uri,
                    'name': name,    
                    'region_list': regions,    
                    'embedding': None,
                })
        return docs

    typesense_collection = "organizations" 
    
    @classmethod
    def typesense_schema(cls):
        schema = {
            'name': cls.typesense_collection,
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'internal_id', 'type': 'int64'},
                {'name': 'region_list', 'type': 'string[]', 'facet': True},
                {'name': 'embedding', 'type': 'float[]', 'num_dim': 768, 'optional': True}, # industry embedding
            ],
            'default_sorting_field': 'internal_id'
        }
        return schema


class CorporateFinanceActivity(ActivityMixin, Resource):
    targetDetails = ArrayProperty(StringProperty())
    valueRaw = ArrayProperty(StringProperty())
    target = RelationshipTo('Organization','target', model=WeightedRel)
    targetName = ArrayProperty(StringProperty())
    vendor = RelationshipTo('Organization','vendor', model=WeightedRel)
    investor = RelationshipFrom('Organization','investor', model=WeightedRel)
    buyer =  RelationshipFrom('Organization', 'buyer', model=WeightedRel)
    protagonist = RelationshipFrom('Organization', 'protagonist', model=WeightedRel)
    participant = RelationshipFrom('Organization', 'participant', model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({
            "vendor": self.vendor,
            "investor": self.investor,
            "buyer": self.buyer,
            "protagonist": self.protagonist,
            "participant": self.participant,
            "target": self.target,
        })

    @property
    def summary_name(self):
        if self.longest_targetName is not None:
            text = f"{self.longest_targetName}"
        else:
            text = ""
        if self.longest_targetDetails is not None:
            text = f"{text} {self.longest_targetDetails}"
        return text
    
    @property
    def longest_targetName(self):
        return longest(self.targetName)

    @property
    def longest_targetDetails(self):
        return longest(self.targetDetails)

    def serialize(self):
        vals = super().serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {
                    "target_details": self.targetDetails,
                    "value_raw": self.valueRaw,
                    "target_name": self.targetName,
                    }
        return {**vals,**act_vals,**activity_mixin_vals}

class PartnershipActivity(ActivityMixin, Resource):
    providedBy = RelationshipTo('Organization','providedBy', model=WeightedRel)
    orgName = ArrayProperty(StringProperty())
    partnership = RelationshipFrom('Organization','partnership', model=WeightedRel)
    awarded = RelationshipFrom('Organization','awarded', model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"provided_by": self.providedBy.all(),
                "partnership": self.partnership.all(),
                "awarded": self.awarded.all(),
                })

    @property
    def summary_name(self):
        return ", ".join(sorted(self.orgName))

    def serialize(self):
        vals = super().serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {"org_names": ", ".join(self.orgName)}
        return {**vals,**act_vals,**activity_mixin_vals}

class RoleActivity(ActivityMixin, Resource):
    orgFoundName = ArrayProperty(StringProperty())
    withRole = RelationshipTo('Role','role', model=WeightedRel)
    roleFoundName = ArrayProperty(StringProperty())
    roleHolderFoundName = ArrayProperty(StringProperty())
    roleActivity = RelationshipFrom('Person','roleActivity', model=WeightedRel)

    @property
    def summary_name(self):
        text = self.longest_activityType
        if text is None:
            return self.longest_roleFoundName
        else:
            return f"{text.title()}: {self.longest_roleFoundName}"

    def related_orgs(self):
        query = f"MATCH (n: RoleActivity {{uri:'{self.uri}'}})--(o: Role)--(p: Organization) RETURN p"
        objs, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in objs for x in sublist]
        return flattened

    @property
    def all_actors(self):
        return self.uniquify({
        "role": self.withRole,
        "person": self.roleActivity,
        "organization": self.related_orgs(),
        })

    @property
    def longest_roleFoundName(self):
        return longest(self.roleFoundName)

    def serialize(self):
        vals = super().serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {
                    "org_found_name": self.orgFoundName,
                    "role_found_name": self.roleFoundName,
                    "role_holder_found_name": self.roleHolderFoundName,
                    }
        return {**vals,**act_vals,**activity_mixin_vals}

class LocationActivity(ActivityMixin, Resource):
    actionFoundName = ArrayProperty(StringProperty())
    locationFoundName = ArrayProperty(StringProperty())
    locationPurpose = ArrayProperty(StringProperty())
    locationType = ArrayProperty(StringProperty())
    orgFoundName = ArrayProperty(StringProperty())
    locationAdded = RelationshipFrom('Organization','locationAdded', model=WeightedRel)
    locationRemoved = RelationshipFrom('Organization','locationRemoved', model=WeightedRel)
    location = RelationshipTo('Site', 'location', model=WeightedRel)

    @property
    def summary_name(self):
        return self.best_name

    @property
    def all_actors(self):
        return self.uniquify({
            "location_added_by": self.locationAdded,
            "location_removed_by": self.locationRemoved,
            "location": self.location,
        })

    @property
    def longest_locationPurpose(self):
        return longest(self.locationPurpose)

    def serialize(self):
        vals = super(LocationActivity, self).serialize()
        activity_fields = self.activity_fields
        act_vals = {
                    "action_found_name": self.actionFoundName,
                    "location_purpose": self.locationPurpose,
                    "location_type": self.locationType,
                    }
        return {**vals,**act_vals,**activity_fields}

class Site(Resource):
    nameClean = StringProperty()
    location = RelationshipFrom('LocationActivity','location', model=WeightedRel)
    nameGeoNamesLocation = RelationshipTo('GeoNamesLocation','nameGeoNamesLocation', model=WeightedRel)
    basedInHighGeoNamesLocation = RelationshipTo('GeoNamesLocation','basedInHighGeoNamesLocation', model=WeightedRel)

    @property
    def based_in_high_geonames_location(self):
        return self.basedInHighGeoNamesLocation
    
    @property
    def nameGeoNamesLocationRDFURL(self):
        return uri_from_related(self.nameGeoNamesLocation)

    @property
    def nameGeoNamesLocationURL(self):
        uri = uri_from_related(self.nameGeoNamesLocation)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri

    @property
    def basedInHighGeoNamesLocationRDFURL(self):
        return uri_from_related(self.basedInHighGeoNamesLocation)

    @property
    def basedInHighGeoNamesLocationURL(self):
        uri = uri_from_related(self.basedInHighGeoNamesLocation)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri


class Person(Resource):
    roleActivity = RelationshipTo('RoleActivity','roleActivity', model=WeightedRel)
    industryClusterPrimary = RelationshipTo('IndustryCluster','industryClusterPrimary', model=WeightedRel)
    industryClusterSecondary = RelationshipTo('IndustryCluster','industryClusterSecondary', model=WeightedRel)


class Role(Resource):
    orgFoundName = ArrayProperty(StringProperty())
    withRole = RelationshipFrom("RoleActivity","role", model=WeightedRel) # prefix needed or doesn't pick up related items
    hasRole = RelationshipFrom('Organization','hasRole', model=WeightedRel)


class ProductActivity(ActivityMixin, Resource):
    productName = ArrayProperty(StringProperty())
    withProduct = RelationshipTo('Product','product', model=WeightedRel)
    productOrganization = RelationshipFrom('Organization','productActivity', model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({
            "product": self.withProduct,
            "organization": self.productOrganization,
        })

    def serialize(self):
        vals = super().serialize()
        activity_fields = self.activity_fields
        act_vals = {
                    "product_name": self.productName,
                    }
        return {**vals,**act_vals,**activity_fields}
    
class Product(Resource):
    useCase = ArrayProperty(StringProperty())
    withProduct = RelationshipFrom("ProductActivity","product", model=WeightedRel)

    def serialize(self):
        vals = super().serialize()
        vals['use_case'] = self.useCase
        return vals

class AboutUs(Resource):
    aboutUs = RelationshipFrom('Organization','hasAboutUs', model=WeightedRel)
    name_embedding_json = JSONProperty()
    
    def to_typesense_doc(self) -> Union[list,dict]:
        docs = []
        related_org_locs = []
        related_org_uris = []
        for related_org in self.aboutUs.filter(internalMergedSameAsHighToUri__isnull=True):
            related_org_locs.extend(related_org.regions)
            related_org_uris.append(related_org.uri)
        jsons = self.name_embedding_json or []
        for idx, embedding in enumerate(jsons):
            docs.append({
                "id": f"{self.internalId}_about_{idx}",
                "internal_id": self.internalId,
                "uri": self.uri,
                "related_org_uris": related_org_uris,
                "region_list": related_org_locs,
                "embedding": embedding,
            })
        return docs
    
    typesense_collection = "about_us"

    @classmethod
    def typesense_schema(cls):
        schema = {
            'name': cls.typesense_collection,
            'fields': [
                {'name': 'internal_id', 'type': 'int64'},
                {'name': 'region_list', 'type': 'string[]', 'facet': True},
                {'name': 'embedding', 'type': 'float[]', 'num_dim': 768},
            ],
            'default_sorting_field': 'internal_id',
        }
        return schema


class AnalystRatingActivity(ActivityMixin, Resource):
    analystRating = RelationshipFrom('Organization','hasAnalystRatingActivity', model=WeightedRel) 

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.analystRating,
                })

class EquityActionsActivity(ActivityMixin, Resource):
    equityAction = RelationshipFrom('Organization','hasEquityActionsActivity', model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.equityAction,
                })

class FinancialReportingActivity(ActivityMixin, Resource):
    financialReporting = RelationshipFrom('Organization','hasFinancialReportingActivity',model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.financialReporting,
                })

class FinancialsActivity(ActivityMixin, Resource):
    financials = RelationshipFrom('Organization','hasFinancialsActivity',model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.financials,
                })
    
class IncidentActivity(ActivityMixin, Resource):
    incident = RelationshipFrom('Organization','hasIncidentActivity',model=WeightedRel) 

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.incident,
                })

class MarketingActivity(ActivityMixin, Resource):
    marketing = RelationshipFrom('Organization','hasMarketingActivity',model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.marketing,
                })

class OperationsActivity(ActivityMixin, Resource):
    operations = RelationshipFrom('Organization','hasOperationsActivity',model=WeightedRel) 

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.operations,
                })

class RecognitionActivity(ActivityMixin, Resource):
    recognition = RelationshipFrom('Organization','hasRecognitionActivity',model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.recognition,
                })

class RegulatoryActivity(ActivityMixin, Resource):
    regulatory = RelationshipFrom('Organization','hasRegulatoryActivity',model=WeightedRel)

    @property
    def all_actors(self):
        return self.uniquify({"organization": self.regulatory,
                })


class IndustrySectorUpdate(Resource):
    mentionedIn = RelationshipFrom('Organization', 'mentionedIn', model=WeightedRel)
    analystOrganization = RelationshipTo('Organization', 'analystOrganization', model=WeightedRel)
    highlight = ArrayProperty(StringProperty())
    documentExtract = StringProperty()
    industry = ArrayProperty(StringProperty())
    industrySubsector = ArrayProperty(StringProperty())
    industry_embedding_json = JSONProperty()
    metric = ArrayProperty(StringProperty())
    whereHighRaw = ArrayProperty(StringProperty())
    whereHighClean = ArrayProperty(StringProperty())
    whereHighGeoNamesLocation = RelationshipTo('GeoNamesLocation','whereHighGeoNamesLocation', model=WeightedRel)

    @property
    def best_highlight(self):
        return longest(self.highlight)
    
    @property
    def best_metric(self):
        return longest(self.metric)
    
    @property
    def best_industry(self):
        return longest(self.industry)
    
    @property
    def best_industrySubsector(self):
        return longest(self.industrySubsector)
    
    def to_typesense_doc(self) -> Union[list,dict]:
        docs = []
        jsons = self.industry_embedding_json or []
        regions = regions_from_geonames(self.whereHighGeoNamesLocation)
        org_uris = [x.uri for x in self.mentionedIn]
        for idx, embedding in enumerate(jsons):
            docs.append({
                "id": f"{self.internalId}_ind_upd_{idx}",
                "uri": self.uri,
                "internal_id": self.internalId,
                "related_org_uris": org_uris, 
                "region_list": list(regions),
                "embedding": embedding,
            })
        return docs
    
    typesense_collection = "industry_sector_updates"

    @classmethod
    def typesense_schema(cls):
        schema = {
            'name': cls.typesense_collection,
            'fields': [
                {'name': 'internal_id', 'type': 'int64'},
                {'name': 'region_list', 'type': 'string[]', 'facet': True},
                {'name': 'embedding', 'type': 'float[]', 'num_dim': 768},
            ],
            'default_sorting_field': 'internal_id',
        }
        return schema