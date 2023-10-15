# Syracuse

Hosted at http://syracuse.1145.am/ with sample data (see `README.md` in the [`dump`](./dump/README.md) directory for details)

## Tech details

Django app with Neo4j backend using neomodel

## Installation

There are some relevant data files saved on [Google Drive](https://drive.google.com/drive/folders/11Iec_wFKkEvRrbmZjUkWNfrZdzqguMBe?usp=sharing) due to their size.

1. Install dependencies: `pipenv install`
2. Copy `relevant_geonames.csv` from Google drive to `dump` folder
3. Setup database. There are 3 options to do this, described in the `README.md` in the [`dump`](./dump/README.md) directory:
   - database dump (from Google drive)
   - cypher file (from Google drive)
   - sample RDF files (in this repo)
