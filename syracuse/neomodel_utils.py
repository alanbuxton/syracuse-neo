from neomodel.properties import Property, validator
from datetime import datetime
import neo4j

# from https://github.com/neo4j-contrib/neomodel/pull/530/
class NativeDateTimeProperty(Property):

    @validator
    def inflate(self, value):
        return value.to_native()

    @validator
    def deflate(self, value):
        if not isinstance(value, datetime):
            raise ValueError(f"datetime object expected, got {type(value)}.")
        return neo4j.time.DateTime.from_native(value)
