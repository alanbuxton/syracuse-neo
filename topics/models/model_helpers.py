from collections import defaultdict
from topics.models import Organization
from topics.industry_geo import orgs_by_industry_and_or_geo


def similar_organizations(organization,limit=0.94,uris_only=False):
        # by industry cluster
        by_ind_cluster = defaultdict(set)
        for x in organization.industryClusterPrimary:
            org_uris = orgs_by_industry_and_or_geo(industry_or_id=x.topicId,geo_code=None)
            if uris_only is True:
                by_ind_cluster[x].update(org_uris)
            else:
                for org_uri in org_uris:
                    org = Organization.self_or_ultimate_target_node(org_uri)
                    if org.uri != organization.uri:
                        by_ind_cluster[x].add(org)
        # by industry texts
        by_ind_text = set()
        if organization.industry is not None:
            for ind in organization.industry:
                org_uris = Organization.by_industry_text(ind,limit=limit)
                if uris_only is True:
                    by_ind_text.update(org_uris)
                else:
                    for org_uri in org_uris:
                        org = Organization.self_or_ultimate_target_node(org_uri)
                        if org.uri != organization.uri and not any([org in x for x in by_ind_cluster.values()]):
                            by_ind_text.add(org)
        for x in organization.sameAsHigh:
            if x.industry is None:
                continue
            for ind in x.industry:
                org_uris = Organization.by_industry_text(ind,limit=limit)
                if uris_only is True:
                    by_ind_text.update(org_uris)
                else:
                    for org_uri in org_uris:
                        org = Organization.self_or_ultimate_target_node(org_uri)
                        if org.uri != organization.uri and not any([org in x for x in by_ind_cluster.values()]):
                            by_ind_text.add(org)
        return {"industry_cluster": dict(by_ind_cluster),
                 "industry_text": by_ind_text}
        
def similar_organizations_flat(organization,limit=0.94,uris_only=False):
    res = similar_organizations(organization,limit=limit,uris_only=uris_only)
    orgs = list(res["industry_text"])
    for vs in res["industry_cluster"].values():
        orgs.extend(vs)
    return orgs