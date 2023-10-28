from neomodel import (StructuredNode, StringProperty,
    RelationshipTo, Relationship, RelationshipFrom,
    DateProperty, db)
from urllib.parse import urlparse
import datetime

def uri_from_related(rel):
    if len(rel) == 0:
        return None
    return rel[0].uri

def geonames_uris():
    qry = "match (n) where n.uri contains ('sws.geonames.org') return n.uri"
    res,_ = db.cypher_query(qry)
    flattened = [x for sublist in res for x in sublist]
    return flattened


class Resource(StructuredNode):
    uri = StringProperty(unique_index=True, required=True)
    foundName = StringProperty()
    name = StringProperty()
    documentTitle = StringProperty()
    documentURL = Relationship('Resource','documentURL')
    sourceName = StringProperty()

    def __hash__(self):
        return hash(self.uri)

    def __eq__(self,other):
        return self.uri == other.uri

    def serialize(self):
        return {
            "entityType": self.__class__.__name__,
            "label": self.name,
            "name": self.name,
            "uri": self.uri,
            "documentTitle": self.documentTitle,
            "documentURL": uri_from_related(self.documentURL),
            "sourceName": self.sourceName,
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

    def same_as(self):
        medium_nodes = self.sameAsMedium.all()
        high_nodes = self.sameAsHigh.all()
        return medium_nodes + high_nodes

    @staticmethod
    def find_same_as_relationships(node_list):
        matching_rels = set()
        tmp_list = set(node_list)
        while len(tmp_list) >= 1:
            node_to_check = tmp_list.pop()
            if hasattr(node_to_check,"sameAsMedium"):
                for candidate_node in node_to_check.sameAsMedium.all():
                    if candidate_node in tmp_list:
                        sorted_nodes = sorted([node_to_check, candidate_node], key=lambda x: x.uri)
                        matching_rels.add( ("sameAsMedium", sorted_nodes[0], sorted_nodes[1]) )
            if hasattr(node_to_check,"sameAsHigh"):
                for candidate_node in node_to_check.sameAsHigh.all():
                    if candidate_node in tmp_list:
                        sorted_nodes = sorted([node_to_check, candidate_node], key=lambda x: x.uri)
                        matching_rels.add( ("sameAsHigh", sorted_nodes[0], sorted_nodes[1])  )
        return matching_rels


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


class ActivityMixin:
    activityType = StringProperty()
    documentDate = DateProperty()
    documentExtract = StringProperty()
    when = DateProperty()
    whenRaw = StringProperty()
    status = StringProperty()
    whereGeoName = StringProperty()
    whereGeoNameRDF = Relationship('Resource','whereGeoNameRDF')

    @property
    def activity_fields(self):
        if self.activityType is None:
            activityType_title = "Activity"
        else:
            activityType_title = self.activityType.title()

        return {
            "activityType": self.activityType,
            "documentDate": self.documentDate,
            "documentExtract": self.documentExtract,
            "label": f"{activityType_title} ({self.sourceName} {self.documentDate})",
            "status": self.status,
            "when": self.when,
            "whenRaw": self.whenRaw,
            "whereGeoName": self.whereGeoName,
            "whereGeoNameURL": self.whereGeoNameURL,
            "whereGeoNameRDFURL": self.whereGeoNameRDFURL,

        }

    @staticmethod
    def by_date(a_date):
        date_plus_one = a_date + datetime.timedelta(days=1)
        cq = f"match (n) where {date_for_cypher(date_plus_one)}) > n.documentDate >= {date_for_cypher(a_date)}) return n"
        res, _ = db.cypher_query(cq, resolve_objects=True)
        flattened = [x for sublist in res for x in sublist]
        return flattened

    def in_date_range(self, from_date, to_date):
        if from_date is None:
            from_date = datetime.date(1,1,1)
        if to_date is None:
            to_date = datetime.date(2999,12,31)
        to_date_plus_one = to_date + datetime.timedelta(days=1)
        constant = datetime.timedelta(days=180)
        to_date_plus_constant = to_date + constant + datetime.timedelta(days=1)
        from_date_minus_constant = from_date - constant

        if self.when and (self.when < to_date_plus_one or self.when >= from_date):
            return True
        elif self.status and self.status == 'has not happened' and (self.documentDate >= from_date_minus_constant and self.documentDate < to_date_plus_one):
            return True
        elif self.status and self.status != 'has not happened' and (self.documentDate >= from_date and self.documentDate < to_date_plus_constant):
            return True
        return False


    @property
    def whereGeoNameRDFURL(self):
        return uri_from_related(self.whereGeoNameRDF)

    @property
    def whereGeoNameURL(self):
        uri = uri_from_related(self.whereGeoNameRDF)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri

    @staticmethod
    def by_country(country_code):
        from .geo_utils import COUNTRY_MAPPING, COUNTRY_CODES # Is 'None' if imported at top of file
        uris = [f"https://sws.geonames.org/{x}/about.rdf" for x in COUNTRY_MAPPING[country_code]]
        resources, _ = db.cypher_query(f"Match (loc)-[:whereGeoNameRDF]-(n) where loc.uri in {uris} return n",resolve_objects=True)
        flattened = [x for sublist in resources for x in sublist]
        return flattened

    @staticmethod
    def orgs_by_activity_where(country_code,limit=None):
        activities = ActivityMixin.by_country(country_code)
        act_uris = [x.uri for x in activities]
        query=f"Match (n: Organization)-[]-(a) where a.uri in {act_uris} return n"
        if limit is not None:
            query = f"{query} limit {limit}"
        orgs, _ = db.cypher_query(query,resolve_objects=True)
        flattened = [x for sublist in orgs for x in sublist]
        return flattened


def date_for_cypher(a_date):
    return f"datetime({{ year: {a_date.year}, month: {a_date.month}, day: {a_date.day} }})"


class BasedInGeoMixin:
    basedInHighGeoName = StringProperty()
    basedInHighGeoNameRDF = Relationship('Resource','basedInHighGeoNameRDF')

    @property
    def basedInHighGeoNameRDFURL(self):
        return uri_from_related(self.basedInHighGeoNameRDF)

    @property
    def basedInHighGeoNameURL(self):
        uri = uri_from_related(self.basedInHighGeoNameRDF)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri

    @staticmethod
    def based_in_country(country_code):
        from .geo_utils import COUNTRY_MAPPING, COUNTRY_CODES # Is 'None' if imported at top of file
        uris = [f"https://sws.geonames.org/{x}/about.rdf" for x in COUNTRY_MAPPING[country_code]]
        resources, _ = db.cypher_query(f"Match (loc)-[:basedInHighGeoNameRDF]-(n) where loc.uri in {uris} return n",resolve_objects=True)
        flattened = [x for sublist in resources for x in sublist]
        return flattened

    @property
    def based_in_fields(self):
        return {"basedInHighGeoName": self.basedInHighGeoName,
                "basedInHighGeoNameURL": self.basedInHighGeoNameURL,
                "basedInHighGeoNameRDFURL": self.basedInHighGeoNameRDFURL}


class Organization(Resource, BasedInGeoMixin):
    description = StringProperty()
    industry = StringProperty()
    investor = RelationshipTo('CorporateFinanceActivity','investor')
    buyer =  RelationshipTo('CorporateFinanceActivity', 'buyer')
    sameAsMedium = Relationship('Organization','sameAsMedium')
    sameAsHigh = Relationship('Organization','sameAsHigh')
    protagonist = RelationshipTo('CorporateFinanceActivity', 'protagonist')
    participant = RelationshipTo('CorporateFinanceActivity', 'participant')
    vendor = RelationshipFrom('CorporateFinanceActivity', 'vendor')
    target = RelationshipFrom('CorporateFinanceActivity', 'target')
    hasRole = RelationshipTo('Role','hasRole')
    locationAdded = RelationshipTo('LocationActivity','locationAdded')
    locationRemoved = RelationshipTo('LocationActivity','locationRemoved')

    @staticmethod
    def find_by_industry(industry):
        return Organization.nodes.filter(industry__regex=rf"(?i).*\b{industry}.*").all()

    @staticmethod
    def get_random():
        return Organization.nodes.order_by('?')[0]

    @staticmethod
    def get_by_uri(uri):
        return Organization.nodes.get(uri)

    def serialize(self):
        vals = super(Organization, self).serialize()
        based_in_fields = self.based_in_fields
        org_vals = {"description": self.description,
                    "industry": self.industry}
        return {**vals,**org_vals,**based_in_fields}

    def get_role_activities(self):
        role_activities = []
        for role in self.hasRole:
            acts = role.withRole.all()
            role_activities.extend([(role,act) for act in acts])
        return role_activities


class CorporateFinanceActivity(Resource, ActivityMixin):
    targetDetails = StringProperty()
    valueRaw = StringProperty()
    target = RelationshipTo('Organization','target')
    targetName = StringProperty()
    vendor = RelationshipTo('Organization','vendor')
    investor = RelationshipFrom('Organization','investor')
    buyer =  RelationshipFrom('Organization', 'buyer')
    protagonist = RelationshipFrom('Organization', 'protagonist')
    participant = RelationshipFrom('Organization', 'participant')

    def serialize(self):
        vals = super(CorporateFinanceActivity, self).serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {
                    "targetDetails": self.targetDetails,
                    "valueRaw": self.valueRaw,
                    "targetName": self.targetName,
                    }
        return {**vals,**act_vals,**activity_mixin_vals}


class RoleActivity(Resource, ActivityMixin):
    orgFoundName = StringProperty()
    withRole = RelationshipTo('Role','role')
    roleFoundName = StringProperty()
    roleHolderFoundName = StringProperty()
    roleActivity = RelationshipFrom('Person','roleActivity')

    def serialize(self):
        vals = super(RoleActivity, self).serialize()
        activity_mixin_vals = self.activity_fields
        act_vals = {
                    "orgFoundName": self.orgFoundName,
                    "roleFoundName": self.roleFoundName,
                    "roleHolderFoundName": self.roleHolderFoundName,
                    }
        return {**vals,**act_vals,**activity_mixin_vals}

class LocationActivity(Resource, ActivityMixin):
    actionFoundName = StringProperty()
    locationFoundName = StringProperty()
    locationPurpose = StringProperty()
    locationType = StringProperty()
    orgFoundName = StringProperty()
    locationAdded = RelationshipFrom('Organization','locationAdded')
    locationRemoved = RelationshipFrom('Organization','locationRemoved')
    location = RelationshipTo('Site', 'location')

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


class Person(Resource, BasedInGeoMixin):
    roleActivity = RelationshipTo('RoleActivity','roleActivity')

    def serialize(self):
        vals = super(Person, self).serialize()
        based_in_fields = self.based_in_fields
        return {**vals,**based_in_fields}

class Role(Resource):
    orgFoundName = StringProperty()
    withRole = RelationshipFrom("RoleActivity","role") # prefix needed or doesn't pick up related items
    hasRole = RelationshipFrom('Organization','hasRole')

class OrganizationSite(Organization, Site):
    __class_name_is_label__ = False

class CorporateFinanceActivityOrganization(Organization, CorporateFinanceActivity):
    __class_name_is_label__ = False

class OrganizationPerson(Organization, Person):
    __class_name_is_label__ = False
