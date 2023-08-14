from neomodel import StructuredNode, StringProperty

class Resource(StructuredNode):
    uri = StringProperty(unique_index=True, required=True)

class Organization(Resource):
    name = StringProperty()
    industry = StringProperty()
    description = StringProperty()
    
