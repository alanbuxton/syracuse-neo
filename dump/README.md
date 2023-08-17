This RDF dumpfile is a small sample of the available data. It is intended as a quick start and is not guaranteed to be comprehensive.

Assuming you have an empty Neo4J v5 database set up for neosemantics (see https://alanbuxton.wordpress.com/2023/04/21/getting-started-with-neo4j-and-neosemantics/) you can import this file with

`call n10s.rdf.import.fetch("file:///path/to/syracuse/dump/sample_dump.ttl","Turtle")`
