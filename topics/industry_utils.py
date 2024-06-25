from topics.models import IndustryCluster
import logging
logger = logging.getLogger(__name__)

def industry_select_list():
    entities = IndustryCluster.leaf_nodes_only()
    if entities is None or len(entities) == 0:
        logger.error("Did not find any Industry Data")
    return entities_to_select_list(entities)

def entities_to_select_list(entities):
    entities = sorted(entities, key=lambda x: x.topicId)
    name_to_id = {}
    for industry in entities:
        if industry.topicId == -1:
            continue
        name, idx = industry.friendly_name_and_id
        name_to_id[name] = idx
    select_list = [(v,k) for k,v in name_to_id.items()]
    sorted_select_list = sorted(select_list, key=lambda x: x[1])
    return sorted_select_list

def industry_keywords(entities):
    kw_to_ind_ids = {}
    id_to_ind_names = {}
    for x in entities:
        for k in x.representation:
            if k not in kw_to_ind_ids:
                kw_to_ind_ids[k] = set()
            kw_to_ind_ids[k].add(x.topicId)
            if x.topicId not in id_to_ind_names:
                id_to_ind_names[x.topicId] = set()
            id_to_ind_names[x.topicId].add(x.uniqueName)
    return kw_to_ind_ids, id_to_ind_names
