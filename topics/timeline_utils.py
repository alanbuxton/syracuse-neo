from topics.models import Article

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

    vendor.extend(allowable_entities(org.vendor,source_names))
    participant.extend(allowable_entities(org.participant,source_names))
    protagonist.extend(allowable_entities(org.protagonist,source_names))
    buyer.extend(allowable_entities(org.buyer,source_names))
    investor.extend(allowable_entities(org.investor,source_names))
    location_added.extend(allowable_entities(org.locationAdded,source_names))
    location_removed.extend(allowable_entities(org.locationRemoved,source_names))
    role_activity.extend(org.get_role_activities(source_names)) # it's a tuple
    target.extend(allowable_entities(org.target,source_names))

    if combine_same_as_name_only is True:
        for x in org.sameAsNameOnly:
            vendor.extend(allowable_entities(x.vendor,source_names))
            participant.extend(allowable_entities(x.participant,source_names))
            buyer.extend(allowable_entities(x.buyer,source_names))
            investor.extend(allowable_entities(x.investor,source_names))
            location_added.extend(allowable_entities(x.locationAdded,source_names))
            location_removed.extend(allowable_entities(x.locationRemoved,source_names))
            role_activity.extend(x.get_role_activities(source_names))
            target.extend(allowable_entities(x.target,source_names))
            
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
                    "label": labelize(current_item,activity_type),
                    "start": item_start.isoformat(),
                    "id": current_item.uri,
                    "className": class_name_for(current_item),
                    })
                item_display_details[current_item.uri] = current_item.serialize_no_none()
                seen_uris.add(current_item.uri)

    return groups, items, item_display_details, org_display_details

def allowable_entities(related_entities, source_names):
    return [x for x in related_entities if x.has_permitted_document_source(source_names)]


def labelize(activity,activity_type):
    if activity.__class__.__name__ == 'RoleActivity':
        if activity.longest_activityType is None:
            label = activity.longest_name.title()
        else:
            label = activity.longest_activityType.title()
        label = f"{label} - {activity.longest_roleFoundName} - {activity.status_as_string}"
    elif activity.__class__.__name__ == 'LocationActivity':
        label = activity.longest_activityType.title()
        fields = ' '.join(filter(None, (activity.longest_name, activity.longest_locationPurpose)))
        label = f"{label} - {fields} - {activity.status_as_string}"
    else:
        label_with_parens = ""
        if activity.longest_activityType is not None:
            label_with_parens = f"({activity.longest_activityType.title()})"
        fields = ' '.join(filter(None, (activity.longest_targetName,activity.longest_targetDetails)))
        label = f"{activity_type.title()} - {fields} - {activity.status_as_string} {label_with_parens}".strip()
    return label

def class_name_for(activity):
    if activity.when is not None:
        return "activity_with_when"
    elif activity.status == "has not happened":
        return "activity_not_happened"
    else:
        return "activity_has_happened"

def snake_case_to_title(text):
    text = text.replace("_"," ")
    return text.title()
