

## Geonames location data

You need to put `relevant_geo.csv` from https://drive.google.com/file/d/1m4l2UyTfC3_ZaUDw1UVazn0ZwAHpY1sb/view?usp=sharing into this folder.


## Neo4j dump

There is a `syracuse-sample-neo4j.dump` avaialable here: https://drive.google.com/file/d/1z4P0iPbTJvYcK8dot_mEMNCS1pMIEQjr/view?usp=sharing that can be imported into a bare Neo4J system. It's small enough to fit into the free Aura DB tier.

If you don't trust a binary file, here is the cypher export that was used to create this database: https://drive.google.com/file/d/15fzpZXGXLydaywcM03xebTw5hpj7UbiC/view?usp=sharing

## RDF

`sample_dump.ttl` dumpfile is a small sample of the available data. This is smaller than the Neo4j dump and is intended as a quick start for loading RDF into your local installation.

Assuming you have an empty Neo4J v5 database set up for neosemantics (see https://alanbuxton.wordpress.com/2023/04/21/getting-started-with-neo4j-and-neosemantics/) you can import this file with:

`python import_export.py dump`
