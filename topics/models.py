from neomodel import (StructuredNode, StringProperty,
    RelationshipTo, Relationship, RelationshipFrom, db, ArrayProperty,
    IntegerProperty, StructuredRel)
from urllib.parse import urlparse
import datetime
from topics.geo_utils import get_geoname_uris_for_country_region
from syracuse.neomodel_utils import NativeDateTimeProperty
import cleanco
from django.core.cache import cache
from syracuse.settings import NEOMODEL_NEO4J_BOLT_URL
from collections import Counter
from neomodel.cardinality import OneOrMore, One

import logging
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

class DocumentSourceRel(StructuredRel):
    documentExtract = StringProperty()

class Resource(StructuredNode):
    uri = StringProperty(unique_index=True, required=True)
    foundName = ArrayProperty(StringProperty())
    name = ArrayProperty(StringProperty())
    documentSource = RelationshipTo("Article","documentSource",
            model=DocumentSourceRel, cardinality=OneOrMore)
    internalDocId = IntegerProperty()
    internalDocIdList = ArrayProperty(IntegerProperty())
    internalNameList = ArrayProperty(StringProperty())
    sameAsMedium = Relationship('Resource','sameAsMedium')
    sameAsHigh = Relationship('Resource','sameAsHigh')
    internalSameAsHighUriList = ArrayProperty(StringProperty())
    internalSameAsMediumUriList = ArrayProperty(StringProperty())

    @property
    def industry_as_str(self):
        pass # override in relevant subclass

    @property
    def basedInHighGeoName_as_str(self):
        pass # override in relevant subclass - if in Mixin make sure Mixin is first https://stackoverflow.com/a/34090986/7414500

    @property
    def whereGeoName_as_str(self):
        pass # override in relevant subclass - if in Mixin make sure Mixin is first https://stackoverflow.com/a/34090986/7414500

    @property
    def earliestDocumentSource(self):
        return self.documentSource.order_by("datePublished")[0]

    @property
    def earliestDatePublished(self):
        return self.earliestDocumentSource.datePublished

    @staticmethod
    def get_by_uri_or_merged_uri(uri):
        r = Resource.nodes.get_or_none(uri=uri)
        if r is not None:
            return r
        return Resource.get_by_merged_uri(uri)

    @staticmethod
    def get_by_merged_uri(uri):
        query = f"""MATCH (n: Resource) WHERE '{uri}' in n.internalSameAsHighUriList OR
                    '{uri}' in n.internalSameAsMediumUriList RETURN n"""
        res, _ = db.cypher_query(query, resolve_objects=True)
        if res is not None and len(res)==1:
            if len(res[0]) == 1:
                return res[0][0]

    def get_merged_same_as_high_by_unmerged_uri(self):
        query = f"""MATCH (n: Resource) WHERE '{self.uri}' in n.internalSameAsHighUriList RETURN n"""
        res, _ = db.cypher_query(query, resolve_objects=True)
        if res is not None and len(res)==1:
            if len(res[0]) == 1:
                return res[0][0]

    def get_merged_same_as_medium_by_unmerged_uri(self):
        query = f"""MATCH (n: Resource) WHERE '{self.uri}' in n.internalSameAsMediumUriList RETURN n"""
        res, _ = db.cypher_query(query, resolve_objects=True)
        if res is not None and len(res)==1:
            if len(res[0]) == 1:
                return res[0][0]

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
            db.set_connection(NEOMODEL_NEO4J_BOLT_URL)

    @classmethod
    def find_by_name(cls, name):
        query = f'match (n: {cls.__name__}) where any (item in n.name where item =~ "(?i).*{name}.*") return *'
        results, columns = db.cypher_query(query)
        return [cls.inflate(row[0]) for row in results]

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

    def __eq__(self,other):
        return self.uri == other.uri

    def serialize(self):
        self.ensure_connection()
        return {
            "entityType": self.__class__.__name__,
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

    def all_directional_relationships(self, **kwargs):
        from_date = kwargs.get('from_date')
        to_date = kwargs.get('to_date')
        all_rels = []
        for key, rel in self.__all_relationships__:
            if isinstance(rel, RelationshipTo):
                direction = "to"
            elif isinstance(rel, RelationshipFrom):
                direction = "from"
            else:
                continue

            if from_date and to_date:
                rels = [(key, direction, x) for x in self.__dict__[key].all() if x.in_date_range(from_date,to_date)]
            else:
                rels = [(key, direction, x) for x in self.__dict__[key].all()]
            all_rels.extend(rels)
        return all_rels

class Article(Resource):
    headline = StringProperty()
    url = RelationshipTo("Resource","url", cardinality=One)
    sourceOrganization = StringProperty()
    datePublished = NativeDateTimeProperty()
    dateRetrieved = NativeDateTimeProperty()

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
        vals = super(Article, self).serialize()
        vals['headline'] = self.best_name
        vals['documentURL'] = self.documentURL
        vals['sourceOrganization'] = self.sourceOrganization
        vals['datePublished'] = self.datePublished
        vals['dateRetrieved'] = self.dateRetrieved
        vals['archiveOrgPageURL'] = self.archiveOrgPageURL
        vals['archiveOrgListURL'] = self.archiveOrgListURL
        return vals


class IndustryCluster(Resource):
    representation = ArrayProperty(StringProperty())
    representativeDoc = ArrayProperty(StringProperty())
    childRight = RelationshipTo("IndustryCluster","childRight")
    childLeft = RelationshipTo("IndustryCluster","childLeft")
    parentsLeft = RelationshipFrom("IndustryCluster","childLeft")
    parentsRight = RelationshipFrom("IndustryCluster","childRight")
    topicId = IntegerProperty()
    uniqueName = StringProperty()
    orgsHigh = RelationshipFrom("Organization","industryClusterHigh")
    orgsMedium = RelationshipFrom("Organization","industryClusterMedium")
    peopleHigh = RelationshipFrom("Person","industryClusterHigh")
    peopleMedium = RelationshipFrom("Person","industryClusterMedium")

    def serialize(self):
        vals = super(IndustryCluster, self).serialize()
        vals['label'] = ", ".join(sorted(self.representation)[:3])
        return vals

    @property
    def parents(self):
        return self.parentsLeft.all() + self.parentsRight.all()

    @staticmethod
    def leaf_keywords():
        cache_key = "industry_leaf_keywords"
        # res = cache.get(cache_key)
        # if res:
        #     return res
        keywords = {}
        for node in IndustryCluster.leaf_nodes_only():
            words = node.uniqueName.split("_")
            idx = words[0]
            for word in words[1:]:
                if word not in keywords.keys():
                    keywords[word] = set()
                keywords[word].add(idx)
        cache.set(cache_key, keywords)
        return keywords

    @staticmethod
    def top_node():
        query = "MATCH (n: IndustryCluster) WHERE NOT (n)<-[:childLeft|childRight]-(:IndustryCluster) AND n.topicId <> -1 return n"
        results, _ = db.cypher_query(query,resolve_objects=True)
        return results[0][0]

    @staticmethod
    def leaf_nodes_only():
        query = "MATCH (n: IndustryCluster) WHERE NOT (n)-[:childLeft|childRight]->(:IndustryCluster) RETURN n"
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
        return IndustryCluster.parents_of(IndustryCluster.leaf_nodes_only())

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


class ActivityMixin:
    activityType = ArrayProperty(StringProperty())
    when = ArrayProperty(NativeDateTimeProperty())
    whenRaw = ArrayProperty(StringProperty())
    status = ArrayProperty(StringProperty())
    whereGeoName = ArrayProperty(StringProperty())
    whereGeoNameRDF = Relationship('Resource','whereGeoNameRDF')
    internalActivityList = ArrayProperty(StringProperty())

    @property
    def whereGeoName_as_str(self):
        return print_friendly(self.whereGeoName)

    @property
    def longest_activityType(self):
        return longest(self.activityType)

    @property
    def status_as_string(self):
        return "; ".join(sorted(self.status))

    @property
    def activity_fields(self):
        if self.activityType is None:
            activityType_title = "Activity"
        else:
            activityType_title = self.longest_activityType.title()
        return {
            "activityType": activityType_title,
            "label": f"{activityType_title} ({self.sourceName}: {self.earliestDatePublished.strftime('%b %Y')})",
            "status": self.status,
            "when": self.when,
            "whenRaw": self.whenRaw,
            "whereGeoName": self.whereGeoName,
            "whereGeoNameURL": self.whereGeoNameURL,
            "whereGeoNameRDFURL": self.whereGeoNameRDFURL,
        }

    @property
    def whereGeoNameRDFURL(self):
        return [x.uri for x in self.whereGeoNameRDF]

    @property
    def whereGeoNameURL(self):
        uris = []
        for x in self.whereGeoNameRDF:
            uri = x.uri.replace("/about.rdf","")
            uris.append(uri)
        return uris

    @staticmethod
    def by_country_or_region(geo_code,allowed_to_set_cache=False):
        cache_key = f"activity_mixin_by_country_{geo_code}"
        res = cache.get(cache_key)
        if res:
            logger.debug(f"{cache_key} cache hit")
            return res
        logger.debug(f"{cache_key} cache miss")
        uris = get_geoname_uris_for_country_region(geo_code)
        resources, _ = db.cypher_query(f"MATCH (loc)-[:whereGeoNameRDF]-(n) WHERE loc.uri IN {uris} RETURN n",resolve_objects=True)
        flattened = [x for sublist in resources for x in sublist]
        if allowed_to_set_cache is True:
            cache.set(cache_key, flattened)
        else:
            logger.debug("Not allowed to set cache")
        return flattened

    @staticmethod
    def orgs_by_activity_where_industry(geo_code=None,industry_uris=None,limit=None,allowed_to_set_cache=False):
        if industry_uris is None:
            hash_key = hash(None)
        else:
            hash_key = hash('_'.join(sorted(industry_uris)))
        cache_key = f"activity_mixin_orgs_by_activity_where_{geo_code}_{hash_key}"
        relevant_items = cache.get(cache_key)
        if relevant_items and limit is None:
            logger.debug(f"{cache_key} cache hit")
            return relevant_items
        logger.debug(f"{cache_key} cache miss")
        if geo_code is not None and geo_code.strip() != '':
            activities = ActivityMixin.by_country_or_region(geo_code,allowed_to_set_cache=allowed_to_set_cache)
            act_uris = [x.uri for x in activities]
            query=f"MATCH (n: Organization)--(a) WHERE a.uri IN {act_uris} "
        else:
            query=f"MATCH (n: Organization) "
        if industry_uris is not None:
            if "WHERE" in query:
                query = f"{query} AND"
            else:
                query = f"{query} WHERE"
            query = f"{query} EXISTS {{ MATCH (n)--(i:IndustryCluster) WHERE i.uri IN {industry_uris} }}"
        query = query + " RETURN n"
        if limit is not None:
            query = f"{query} LIMIT {limit}"
        orgs, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in orgs for x in sublist]
        if allowed_to_set_cache is True:
            cache.set(cache_key, flattened)
        else:
            logger.debug("Not allowed to set cache")
        return flattened


def date_for_cypher(a_date):
    return f"datetime({{ year: {a_date.year}, month: {a_date.month}, day: {a_date.day} }})"


class BasedInGeoMixin:
    basedInHighGeoName = ArrayProperty(StringProperty())
    basedInHighGeoNameRDF = Relationship('Resource','basedInHighGeoNameRDF')

    @property
    def basedInHighGeoName_as_str(self):
        return print_friendly(self.basedInHighGeoName)

    @property
    def basedInHighGeoNameRDFURL(self):
        return uri_from_related(self.basedInHighGeoNameRDF)

    @property
    def basedInHighGeoNameURL(self):
        uri = uri_from_related(self.basedInHighGeoNameRDF)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri

    @classmethod
    def by_country_region_industry(cls,geo_code=None,industry_uris=None,limit=20,allowed_to_set_cache=False):
        label = cls.__name__
        if industry_uris is None:
            hash_key = hash(None)
        else:
            hash_key = hash('_'.join(sorted(industry_uris)))
        cache_key = f"{label}_based_in_country_or_region_{geo_code}_{hash_key}"
        res = cache.get(cache_key)
        if res:
            logger.debug(f"{cache_key} cache hit")
            return res
        logger.debug(f"{cache_key} cache miss")
        if geo_code is not None and geo_code.strip() != '':
            geo_uris = get_geoname_uris_for_country_region(geo_code)
            query = f"MATCH (loc)-[:basedInHighGeoNameRDF]-(n: {label}) WHERE loc.uri IN {geo_uris} "
        else:
            query = f"MATCH (n: {label})"
        if industry_uris is not None:
            if "WHERE" in query:
                query = f"{query} AND"
            else:
                query = f"{query} WHERE"
            query = f"{query} EXISTS {{ MATCH (n)--(i:IndustryCluster) WHERE i.uri IN {industry_uris} }}"
        query = f"{query} RETURN n"
        if limit is not None:
            query = f"{query} LIMIT {limit}"
        resources, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in resources for x in sublist]
        if allowed_to_set_cache is True:
            cache.set(cache_key, flattened)
        else:
            logger.debug("Not allowed to update cache")
        return flattened

    @property
    def based_in_fields(self):
        return {"basedInHighGeoName": self.basedInHighGeoName,
                "basedInHighGeoNameURL": self.basedInHighGeoNameURL,
                "basedInHighGeoNameRDFURL": self.basedInHighGeoNameRDFURL,
                # "basedInHighGeoNameAsStr": self.basedInHighGeoName_as_str,
                }


class Organization(BasedInGeoMixin, Resource):
    description = ArrayProperty(StringProperty())
    industry = ArrayProperty(StringProperty())
    investor = RelationshipTo('CorporateFinanceActivity','investor')
    buyer =  RelationshipTo('CorporateFinanceActivity', 'buyer')
    protagonist = RelationshipTo('CorporateFinanceActivity', 'protagonist')
    participant = RelationshipTo('CorporateFinanceActivity', 'participant')
    vendor = RelationshipFrom('CorporateFinanceActivity', 'vendor')
    target = RelationshipFrom('CorporateFinanceActivity', 'target')
    hasRole = RelationshipTo('Role','hasRole')
    locationAdded = RelationshipTo('LocationActivity','locationAdded')
    locationRemoved = RelationshipTo('LocationActivity','locationRemoved')
    industryClusterHigh = RelationshipTo('IndustryCluster','industryClusterHigh')
    industryClusterMedium = RelationshipTo('IndustryCluster','industryClusterMedium')

    @property
    def industry_as_str(self):
        if self.industry is None:
            return None
        industry_as_titles = [ x.title() for x in self.industry ]
        return print_friendly(industry_as_titles)

    @property
    def best_name(self):
        if self.internalNameList is None or len(self.internalNameList) == 0:
            return self.longest_name
        name_cands = [x.split("_##_")[1] for x in self.internalNameList]
        name_counter = Counter(name_cands)
        return name_counter.most_common(1)[0][0]

    @staticmethod
    def get_best_name_by_uri(uri):
        org = Organization.get_by_uri_or_merged_uri(uri)
        if org is not None:
            return org.best_name

    @staticmethod
    def by_uris(uris):
        return Organization.nodes.filter(uri__in=uris)

    @classmethod
    def find_by_industry(cls, industry):
        query = f'match (n: {cls.__name__}) where any (item in n.industry where item =~ "(?i).*{industry}.*") return *'
        results, columns = db.cypher_query(query)
        return [cls.inflate(row[0]) for row in results]

    @staticmethod
    def get_random():
        return Organization.nodes.order_by('?')[0]

    @staticmethod
    def get_by_uri(uri):
        return Organization.nodes.get(uri)

    @property
    def shortest_name_length(self):
        words = shortest(self.name)
        if words is None:
            return 0
        return len(cleanco.basename(words).split())

    def serialize(self):
        vals = super(Organization, self).serialize()
        based_in_fields = self.based_in_fields
        org_vals = {"description": self.description,
                    "industry": self.industry,
#                    "industryAsStr": self.industry_as_str,
                    }
        return {**vals,**org_vals,**based_in_fields}

    def get_role_activities(self):
        role_activities = []
        for role in self.hasRole:
            acts = role.withRole.all()
            role_activities.extend([(role,act) for act in acts])
        return role_activities

    def same_as(self,min_name_length=2):
        high_matches = set(self.same_as_highs())
        med_matches = set()
        if (self.name is not None and not isinstance(self.name, str) and
                all([len(x.split()) >= min_name_length for x in self.name])):
            med_matches = self.same_as_mediums(min_name_length)
        return high_matches.union(med_matches)

    @staticmethod
    def find_same_as_relationships(node_list, min_name_length=2):
        matching_rels = set()
        for node in node_list:
            if not isinstance(node, Organization):
                logger.debug(f"Can't do sameAs for {node}")
                continue
            matches = node.same_as_highs()
            for match in matches:
                matching_rels.add( ("sameAsHigh", node, match) )
            if (node.name is not None and not isinstance(node.name, str) and
                    all([len(x.split()) >= min_name_length for x in node.name])):
                med_matches = node.same_as_mediums(min_name_length=min_name_length)
                for med_match in med_matches:
                    matching_rels.add( ("sameAsMedium", node, med_match) )
        return matching_rels

    def same_as_highs(self):
        return self.related_by("sameAsHigh")

    def related_by(self,relationship):
        query = f'''
            match (n: {self.__class__.__name__} {{ uri:"{self.uri}"}})
            CALL apoc.path.subgraphNodes(n, {{
            	relationshipFilter: "{relationship}",
                labelFilter: "{self.__class__.__name__}",
                minLevel: 1,
                maxLevel: 5
            }})
            YIELD node
            RETURN node;
        '''
        nodes, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in nodes for x in sublist]
        return flattened

    def same_as_mediums(self, min_name_length, max_iterations=5):
        found_nodes = set()
        new_nodes = [self]
        for r in range(0,max_iterations):
            new_nodes = self.same_as_mediums_by_length(min_name_length)
            new_nodes = [n for n in new_nodes if n not in found_nodes]
            found_nodes.update(new_nodes)
        return found_nodes

    def same_as_mediums_by_length(self, min_name_length):
        new_nodes = [node for node in self.sameAsMedium if node.shortest_name_length >= min_name_length]
        return new_nodes


class CorporateFinanceActivity(ActivityMixin, Resource):
    targetDetails = ArrayProperty(StringProperty())
    valueRaw = ArrayProperty(StringProperty())
    target = RelationshipTo('Organization','target')
    targetName = ArrayProperty(StringProperty())
    vendor = RelationshipTo('Organization','vendor')
    investor = RelationshipFrom('Organization','investor')
    buyer =  RelationshipFrom('Organization', 'buyer')
    protagonist = RelationshipFrom('Organization', 'protagonist')
    participant = RelationshipFrom('Organization', 'participant')

    @property
    def all_participants(self):
        return {
            "vendor": self.vendor.all(),
            "investor": self.investor.all(),
            "buyer": self.buyer.all(),
            "protagonist": self.protagonist.all(),
            "participant": self.participant.all(),
        }

    @property
    def longest_targetName(self):
        return longest(self.targetName)

    @property
    def longest_targetDetails(self):
        return longest(self.targetDetails)

    def serialize(self):
        vals = super(CorporateFinanceActivity, self).serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {
                    "targetDetails": self.targetDetails,
                    "valueRaw": self.valueRaw,
                    "targetName": self.targetName,
                    }
        return {**vals,**act_vals,**activity_mixin_vals}


class RoleActivity(ActivityMixin, Resource):
    orgFoundName = ArrayProperty(StringProperty())
    withRole = RelationshipTo('Role','role')
    roleFoundName = ArrayProperty(StringProperty())
    roleHolderFoundName = ArrayProperty(StringProperty())
    roleActivity = RelationshipFrom('Person','roleActivity')

    def related_orgs(self):
        query = f"MATCH (n: RoleActivity {{uri:'{self.uri}'}})--(o: Role)--(p: Organization) RETURN p"
        objs, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in objs for x in sublist]
        return flattened

    @property
    def all_participants(self):
        return {
        "role": self.withRole.all(),
        "person": self.roleActivity.all(),
        "organization": self.related_orgs(),
        }

    @property
    def longest_roleFoundName(self):
        return longest(self.roleFoundName)

    def serialize(self):
        vals = super(RoleActivity, self).serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {
                    "orgFoundName": self.orgFoundName,
                    "roleFoundName": self.roleFoundName,
                    "roleHolderFoundName": self.roleHolderFoundName,
                    }
        return {**vals,**act_vals,**activity_mixin_vals}

class LocationActivity(ActivityMixin, Resource):
    actionFoundName = ArrayProperty(StringProperty())
    locationFoundName = ArrayProperty(StringProperty())
    locationPurpose = ArrayProperty(StringProperty())
    locationType = ArrayProperty(StringProperty())
    orgFoundName = ArrayProperty(StringProperty())
    locationAdded = RelationshipFrom('Organization','locationAdded')
    locationRemoved = RelationshipFrom('Organization','locationRemoved')
    location = RelationshipTo('Site', 'location')

    @property
    def all_participants(self):
        return {
            "location_added_by": self.locationAdded.all(),
            "location_removed_by": self.locationRemoved.all(),
            "location": self.location.all(),
        }

    @property
    def longest_locationPurpose(self):
        return longest(self.locationPurpose)

    def serialize(self):
        vals = super(LocationActivity, self).serialize()
        activity_fields = self.activity_fields
        act_vals = {
                    "actionFoundName": self.actionFoundName,
                    "locationPurpose": self.locationPurpose,
                    "locationType": self.locationType,
                    }
        return {**vals,**act_vals,**activity_fields}

class Site(Resource):
    nameGeoName = StringProperty()
    nameGeoNameRDF = Relationship('Resource','nameGeoNameRDF')
    location = RelationshipFrom('LocationActivity','location')

    @property
    def nameGeoNameRDFURL(self):
        return uri_from_related(self.nameGeoNameRDF)

    @property
    def nameGeoNameURL(self):
        uri = uri_from_related(self.nameGeoNameRDF)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri


class Person(BasedInGeoMixin, Resource):
    roleActivity = RelationshipTo('RoleActivity','roleActivity')
    industryClusterHigh = RelationshipTo('IndustryCluster','industryClusterHigh')
    industryClusterMedium = RelationshipTo('IndustryCluster','industryClusterMedium')

    def serialize(self):
        vals = super(Person, self).serialize()
        based_in_fields = self.based_in_fields
        return {**vals,**based_in_fields}

class Role(Resource):
    orgFoundName = ArrayProperty(StringProperty())
    withRole = RelationshipFrom("RoleActivity","role") # prefix needed or doesn't pick up related items
    hasRole = RelationshipFrom('Organization','hasRole')

class OrganizationSite(Organization, Site):
    __class_name_is_label__ = False

class CorporateFinanceActivityOrganization(Organization, CorporateFinanceActivity):
    __class_name_is_label__ = False

class OrganizationPerson(Organization, Person):
    __class_name_is_label__ = False


def print_friendly(vals, limit = 2):
    if vals is None:
        return None
    first_n = vals[:limit]
    val = ", ".join(first_n)
    extras = len(vals[limit:])
    if extras > 0:
        val = f"{val} and {extras} more"
    return val
