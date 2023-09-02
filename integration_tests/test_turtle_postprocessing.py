from django.test import  TestCase
from neomodel import db
import time
from import_export import apoc_del_redundant_high_med

class TurtlePostProcessingTestCase(TestCase):
    def setUp(self):
        ts = time.time()
        uri1 = f"http://{ts}/foo"
        uri2 = f"http://{ts}/bar"
        uri3 = f"http://{ts}/baz"

        db.cypher_query(f'''
            CREATE
            (a:Organization {{uri:"{uri1}",name:"foo"}}),
            (b:Organization {{uri:"{uri2}",name:"bar"}}),
            (c:Organization {{uri:"{uri3}",name:"baz"}}),
            (a)-[:sameAsHigh]->(b)-[:sameAsHigh]->(a),
            (a)-[:sameAsHigh]->(c)-[:sameAsHigh]->(a),
            (b)-[:sameAsHigh]->(c)-[:sameAsHigh]->(b)
            RETURN *
        ''')

        self.uri1 = uri1
        self.uri2 = uri2
        self.uri3 = uri3

    def test_deletes_not_needed_same_as(self):
        uri1 = self.uri1 # Just for convenience in case need to copy paste items below into shell
        uri2 = self.uri2
        uri3 = self.uri3

        counts1, _ = db.cypher_query(f'MATCH (n {{uri:"{uri1}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts2, _ = db.cypher_query(f'MATCH (n {{uri:"{uri2}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts3, _ = db.cypher_query(f'MATCH (n {{uri:"{uri3}"}})-[o:sameAsHigh]-(p) return count(o)' )

        assert counts1[0][0] == 4
        assert counts2[0][0] == 4
        assert counts3[0][0] == 4

        apoc_del_redundant_high_med()

        counts1_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri1}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts2_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri2}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts3_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri3}"}})-[o:sameAsHigh]-(p) return count(o)' )

        assert counts1_after[0][0] == 2
        assert counts2_after[0][0] == 2
        assert counts3_after[0][0] == 2
