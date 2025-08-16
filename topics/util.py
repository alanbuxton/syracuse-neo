import re
import string
from urllib.parse import urlparse
import cleanco

ORG_ACTIVITY_LIST="|".join([f"{x}Activity" for x in ["CorporateFinance","Product","Location","Partnership","AnalystRating","EquityActions","EquityActions","FinancialReporting","Financials","Incident","Marketing","Operations","Recognition","Regulatory"]])
ALL_ACTIVITY_LIST= ORG_ACTIVITY_LIST + "|RoleActivity"

def clean_punct(text,replacement=' '):
    return re.sub(rf"[{string.punctuation} ]",replacement,text)

def standardize_name(name):
    name = cleanco.basename(name)
    name = clean_punct(name)
    name = re.sub(r"\s{2,}", "", name)
    name = name.lower()
    return name

def blank_or_none(val):
    if val is None:
        return True
    if isinstance(val, str) and val.strip() == '':
        return True
    return False

def geo_to_country_admin1(geo_code):
    if geo_code is None: 
        return None, None
    splitted = geo_code.split("-")
    country_code = splitted[0]
    admin1_code = splitted[1] if len(splitted) > 1 else None
    return country_code, admin1_code

def elements_from_uri(uri):
    if uri is None or uri == '':
        return {}
    parsed = urlparse(uri)
    part_pieces = parsed.path.split("/")
    path = part_pieces[1]
    doc_id = part_pieces[2]
    org_name = "/".join(part_pieces[3:])
    return {
        "domain": parsed.netloc,
        "path": path,
        "doc_id": doc_id,
        "name": org_name,
    }


def camel_case_to_snake_case(text):
    text = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', text))
    text = re.sub(r'\s+','_', text)
    return text.lower()


