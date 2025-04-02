from topics.models import Article
from .organization_search_helpers import get_same_as_name_onlies

def get_timeline_data(org,combine_same_as_name_only, 
                      source_names = Article.core_sources(),
                      min_date = None):
    org_display = []
    org_nodes = []
    activities = []
 
    display_data = org.serialize()
    org_display.append(display_data)
    org_nodes.append(org)
    vendor = []
    participant = []
    protagonist = []
    buyer = []
    investor = []
    role_activity = []
    location_added = []
    location_removed = []
    target = []
    partnership = []
    awarded = []
    provided_by = []

    vendor.extend(allowable_entities(org.vendor,source_names))
    participant.extend(allowable_entities(org.participant,source_names))
    protagonist.extend(allowable_entities(org.protagonist,source_names))
    buyer.extend(allowable_entities(org.buyer,source_names))
    investor.extend(allowable_entities(org.investor,source_names))
    location_added.extend(allowable_entities(org.locationAdded,source_names))
    location_removed.extend(allowable_entities(org.locationRemoved,source_names))
    role_activity.extend(org.get_role_activities(source_names)) # it's a tuple
    target.extend(allowable_entities(org.target,source_names))
    partnership.extend(allowable_entities(org.partnership, source_names))
    awarded.extend(allowable_entities(org.awarded, source_names))
    provided_by.extend(allowable_entities(org.providedBy, source_names))

    if combine_same_as_name_only is True:
        for x in get_same_as_name_onlies(org):
            vendor.extend(allowable_entities(x.vendor,source_names))
            participant.extend(allowable_entities(x.participant,source_names))
            buyer.extend(allowable_entities(x.buyer,source_names))
            investor.extend(allowable_entities(x.investor,source_names))
            location_added.extend(allowable_entities(x.locationAdded,source_names))
            location_removed.extend(allowable_entities(x.locationRemoved,source_names))
            role_activity.extend(x.get_role_activities(source_names))
            target.extend(allowable_entities(x.target,source_names))
            partnership.extend(allowable_entities(x.partnership, source_names))
            awarded.extend(allowable_entities(x.awarded, source_names))
            provided_by.extend(allowable_entities(x.providedBy, source_names))
            
    activities.append(
        {"vendor": set(vendor),
        "investor": set(investor),
        "participant": set(participant),
        "protagonist": set(protagonist),
        "buyer": set(buyer),
        "location_added": set(location_added),
        "location_removed": set(location_removed),
        "role_activity": set(role_activity),
        "target": set(target),
        "partnership": set(partnership),
        "awarded": set(awarded),
        "provided_by": set(provided_by)
        })

    groups = []
    org_display_details = {}
    activity_to_subgroup = {
        "vendor": "corporate_finance",
        "investor": "corporate_finance",
        "participant": "corporate_finance",
        "protagonist": "corporate_finance",
        "buyer": "corporate_finance",
        "location_added": "location",
        "location_removed": "location",
        "role_activity": "role",
        "target": "corporate_finance",
        "awarded": "partnership",
        "partnership": "partnership",
        "provided_by": "partnership",
    }
    item_display_details = {}
    items = []
    seen_uris = set()

    for idx, x in enumerate(org_display):
        l1_group = {"id": idx, "content": x["label"], "treeLevel": 1, "nestedGroups": []}
        org_display_details[idx] = x
        for l2 in sorted(set(activity_to_subgroup.values())):
            l2_id = f"{idx}-{l2}"
            groups.append( {"id": l2_id, "content": snake_case_to_title(l2), "treeLevel": 2})
            l1_group["nestedGroups"].append(l2_id)
        groups.append(l1_group)

    for idx,activity in enumerate(activities):
        for activity_type,vs in activity.items():
            for v in vs:
                if isinstance(v, tuple):
                    current_item = v[1]
                else:
                    current_item = v
                if current_item.uri in seen_uris:
                    continue
                item_start = current_item.earliestDatePublished
                if min_date is not None and item_start.date() < min_date:
                    continue
                l2_id = f"{idx}-{activity_to_subgroup[activity_type]}"
                items.append(
                    {"group": l2_id,
                    "label": labelize(current_item,activity_type,activity_to_subgroup[activity_type]),
                    "start": item_start.isoformat(),
                    "id": current_item.uri,
                    })
                item_display_details[current_item.uri] = current_item.serialize_no_none()
                seen_uris.add(current_item.uri)

    return groups, items, item_display_details, org_display_details


def allowable_entities(related_entities, source_names):
    return [x for x in related_entities if x.has_permitted_document_source(source_names)]


def labelize(activity,activity_type,subgroup):
    name = activity.summary_name.title()
    if subgroup == 'corporate_finance':
        return f"{name} ({snake_case_to_title(activity_type)})"
    else:
        return name


def snake_case_to_title(text):
    text = text.replace("_"," ")
    return text.title()
