# Purpose of this folder

This folder must contain a `relevant_geo.csv` file for your installation to run.

It also offers some example `ttl` (RDF) files for illustration purposes, but most likely you will want to populate your local Neo4j database either with the cypher file or dump.

The relevant files are in Google Drive due to their size: https://drive.google.com/drive/folders/11Iec_wFKkEvRrbmZjUkWNfrZdzqguMBe?usp=sharing

## Geonames location data

Download [`relevant_geo.csv`](https://drive.google.com/file/d/1m4l2UyTfC3_ZaUDw1UVazn0ZwAHpY1sb/view?usp=sharing) into this folder.


## Loading your database

### Neo4j dump

There is a [`syracuse-sample-neo4j.dump`](https://drive.google.com/file/d/1z4P0iPbTJvYcK8dot_mEMNCS1pMIEQjr/view?usp=sharing) that can be imported into a bare Neo4J system. It's small enough to fit into the free Aura DB tier.

### Cypher file

If you don't trust a binary file, here is the [cypher export](https://drive.google.com/file/d/15fzpZXGXLydaywcM03xebTw5hpj7UbiC/view?usp=sharing) that was used to create this database. You can import this cypher export with
`cat /path/to/syracuse-sample-neo4j.cypher | ./bin/cypher-shell -u neo4j -p <password>`

### RDF

`*.ttl` dumpfiles within the timestamped subdirectory are a tiny sample of the available data. They are far smaller than the Neo4j dump and are intended as a quick start for loading RDF into your local installation.

Assuming you have an empty Neo4J v5 database with APOC and neosemantics plugins installed you can import this file with:

`python manage.py import_ttl -d dump`

If you find that this command tells you "No new TTL files to import" and you want to reload these files, you will need to:

1. Clean your neo4j database - in a cypher shell: "Match (n) detach delete n"
2. Delete the DataImport object(s) from your postgres - in a django shell:
  ```
  from integration.models import DataImport
  DataImport.objects.all().delete()
  ```
