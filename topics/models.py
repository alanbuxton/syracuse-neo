from neomodel import StructuredNode, StringProperty, RelationshipTo, Relationship, RelationshipFrom
from urllib.parse import urlparse


def uri_from_related(rel):
    if len(rel) == 0:
        return None
    return rel[0].uri


class Resource(StructuredNode):
    uri = StringProperty(unique_index=True, required=True)
    foundName = StringProperty()
    name = StringProperty()
    documentTitle = StringProperty()
    documentURL = Relationship('Resource','documentURL')

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
        while len(tmp_list) > 1:
            node_to_check = tmp_list.pop()
            if not hasattr(node_to_check,"sameAsMedium"):
                return set()
            for candidate_node in node_to_check.sameAsMedium.all():
                if candidate_node in tmp_list:
                    sorted_nodes = sorted([node_to_check, candidate_node], key=lambda x: x.uri)
                    matching_rels.add( ("sameAsMedium", sorted_nodes[0], sorted_nodes[1]) )
            for candidate_node in node_to_check.sameAsHigh.all():
                if candidate_node in tmp_list:
                    sorted_nodes = sorted([node_to_check, candidate_node], key=lambda x: x.uri)
                    matching_rels.add( ("sameAsHigh", sorted_nodes[0], sorted_nodes[1])  )
        return matching_rels


    def all_directional_relationships(self):
        all_rels = []
        for key, rel in self.__all_relationships__:
            if isinstance(rel, RelationshipTo):
                direction = "to"
            elif isinstance(rel, RelationshipFrom):
                direction = "from"
            else:
                continue

            rels = [(key, direction, x) for x in self.__dict__[key].all()]
            all_rels.extend(rels)
        return all_rels



class ActivityMixin:
    activityType = StringProperty()
    documentDate = StringProperty()
    documentExtract = StringProperty()


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

class WhereGeoMixin:
    whereGeoName = StringProperty()
    whereGeoNameRDF = Relationship('Resource','whereGeoNameRDF')

    @property
    def whereGeoNameRDFURL(self):
        return uri_from_related(self.whereGeoNameRDF)

    @property
    def whereGeoNameURL(self):
        uri = uri_from_related(self.whereGeoNameRDF)
        if uri:
            uri = uri.replace("/about.rdf","")
        return uri

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

    @staticmethod
    def get_random():
        return Organization.nodes.order_by('?')[0]

    @staticmethod
    def get_by_uri(uri):
        return Organization.nodes.get(uri)

    def serialize(self):
        vals = super(Organization, self).serialize()
        org_vals = {"description": self.description,
                    "industry": self.industry,
                    "basedInHighGeoName": self.basedInHighGeoName,
                    "basedInHighGeoNameURL": self.basedInHighGeoNameURL,
                    "basedInHighGeoNameRDFURL": self.basedInHighGeoNameRDFURL}
        return {**vals,**org_vals}


class CorporateFinanceActivity(Resource, ActivityMixin, WhereGeoMixin):
    status = StringProperty()
    targetDetails = StringProperty()
    valueRaw = StringProperty()
    when = StringProperty()
    whenRaw = StringProperty()
    target = RelationshipTo('Organization','target')
    targetName = StringProperty()
    vendor = RelationshipTo('Organization','vendor')
    investor = RelationshipFrom('Organization','investor')
    buyer =  RelationshipFrom('Organization', 'buyer')
    protagonist = RelationshipFrom('Organization', 'protagonist')
    participant = RelationshipFrom('Organization', 'participant')

    def serialize(self):
        vals = super(CorporateFinanceActivity, self).serialize()
        act_vals = {"activityType":self.activityType,
                    "documentDate": self.documentDate,
                    "documentExtract": self.documentExtract,
                    "status": self.status,
                    "targetDetails": self.targetDetails,
                    "valueRaw": self.valueRaw,
                    "when": self.when,
                    "whenRaw": self.whenRaw,
                    "targetName": self.targetName,
                    "whereGeoName": self.whereGeoName,
                    "whereGeoNameURL": self.whereGeoNameURL,
                    "whereGeoNameRDFURL": self.whereGeoNameRDFURL,
                    }
        return {**vals,**act_vals}

class RoleActivity(Resource, ActivityMixin):
    orgFoundName = StringProperty()
    role = RelationshipTo('Role','role')
    roleFoundName = StringProperty()
    roleHolderFoundName = StringProperty()


class SiteAddedActivity(Resource, ActivityMixin):
    pass

class SiteRemovedActivity(Resource, ActivityMixin):
    pass

class ProductAddedActivity(Resource, ActivityMixin):
    pass

class Site(Resource):
    pass

class Person(Resource, BasedInGeoMixin):
    roleActivity = RelationshipTo('Role','roleActivity')

class Role(Resource):
    orgFoundName = StringProperty()
