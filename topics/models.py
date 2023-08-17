from neomodel import StructuredNode, StringProperty, RelationshipTo, Relationship, RelationshipFrom
from urllib.parse import urlparse

class Resource(StructuredNode):
    uri = StringProperty(unique_index=True, required=True)
    foundName = StringProperty()
    name = StringProperty()
    documentTitle = StringProperty()
    documentURL = StringProperty()

    def serialize(self):
        return {
            "entity_type": self.__class__.__name__,
            "label": self.name,
            "name": self.name,
            "uri": self.uri,
            "documentTitle": self.documentTitle,
            "documentURL": self.documentURL,
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
            "name": splitted_uri[3]
        }

    def all_relationships(self):
        all_rels = []
        for key, rel in self.__all_relationships__:
            if isinstance(rel, RelationshipTo):
                direction = "to"
            elif isinstance(rel, RelationshipFrom):
                direction = "from"
            else:
                direction = "none"

            rels = [(key, direction, x) for x in self.__dict__[key].all()]
            all_rels.extend(rels)
        return all_rels


class ActivityMixin:
    activityType = StringProperty()
    documentDate = StringProperty()
    documentExtract = StringProperty()


class BasedInGeoMixin:
    basedInHighGeoName = StringProperty()
    basedInHighGeoNameRDF = StringProperty() #Relationship('Resource','basedInHighGeoNameRDF')

class WhereGeoMixin:
    whereGeoName = StringProperty()
    whereGeoNameRDF = StringProperty() #RelationshipTo('Resource','whereGeoNameRDF')

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
                    "industry": self.industry}
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
    investor = RelationshipFrom('CorporateFinanceActivity','investor')
    buyer =  RelationshipFrom('CorporateFinanceActivity', 'buyer')
    protagonist = RelationshipFrom('CorporateFinanceActivity', 'protagonist')
    participant = RelationshipFrom('CorporateFinanceActivity', 'participant')

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
                    "targetName": self.targetName}
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
