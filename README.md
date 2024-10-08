# Syracuse

Hosted at http://syracuse.1145.am/

## Tech details

Django app with Neo4j backend using neomodel

Also requires Postgres for user administration etc

## Installation

1. Install dependencies: `pipenv install`
2. Copy `relevant_geonames.csv` from Google drive to the `dump` folder. See `README.md` in the [`dump`](./dump/README.md) directory for details.
3. Make sure your Neo4j database has the `apoc5plus` and `n10s` plugins installed.
4. Set up local Postgres and apply migrations `python manage.py migrate`
5. Set up the neo4j database by importing the RDF dumpfiles in the `dump` folder. For a fresh installation this needs `python manage.py import_ttl -d dump -a`

If you find that this command tells you "No new TTL files to import" and you want to reload these files, you will need to clean your Neo4j database and delete the DataImport object(s) from your Postgres. In a Django shell do the following:

```
from integration.models import DataImport
from neomodel import db
db.cypher_query("Match (n) detach delete n")
DataImport.objects.all().delete()
```
