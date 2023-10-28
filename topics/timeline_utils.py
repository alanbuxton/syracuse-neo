from topics.graph_utils import get_node_cluster
from topics.models import Organization

def get_timeline_data(orgs, limit=None):
    org_cluster_display = []
    org_cluster_nodes = []
    activities = []
    seen_orgs = set()
    errors = set()
    org_count = 0

    for org in orgs:
        if limit is not None and org_count > limit:
            break
        if org in seen_orgs:
            continue
        res = get_node_cluster(org)
        if isinstance(res, dict) and res.get("error_names") is not None:
            errors.add(org.name)
            seen_orgs.update(res["error_orgs"])
            continue
        org_count += 1
        display_data,node_cluster,_ = res
        display_data['label'] = org.name
        display_data['uri'] = org.uri
        seen_orgs.update(node_cluster)
        org_cluster_display.append(display_data)
        org_cluster_nodes.append(node_cluster)
        vendor = []
        participant = []
        protagonist = []
        buyer = []
        investor = []
        for node in node_cluster:
            vendor.extend(node.vendor.all())
            participant.extend(node.participant.all())
            protagonist.extend(node.protagonist.all())
            buyer.extend(node.buyer.all())
            investor.extend(node.investor.all())
        activities.append(
            {"vendor": set(vendor),
             "investor": set(investor),
             "participant": set(participant),
             "protagonist": set(protagonist),
             "buyer": set(buyer)
             })

    groups = []
    org_display_details = {}
    for idx, x in enumerate(org_cluster_display):
        groups.append( {"id": idx, "content": x["label"]})
        org_display_details[idx] = x

    items = []
    item_display_details = {}

    seen_uris = set()

    for idx,activity in enumerate(activities):
        for activity_type,vs in activity.items():
            for v in vs:
                if v.uri in seen_uris:
                    continue
                items.append(
                    {"group": idx,
                    "label": labelize(v,activity_type),
                    "start": v.documentDate,
                    "id": v.uri,
                    "className": class_name_for(v),
                    })
                item_display_details[v.uri] = v.serialize_no_none()
                seen_uris.add(v.uri)

    return groups, items, item_display_details, org_display_details, errors


def labelize(activity,activity_type ):
    label = activity.activityType
    if activity.targetName is not None:
        label = f"{label} - {activity.targetName}"
    if activity.targetDetails is not None:
        label = f"{label} {activity.targetDetails}"
    label = f"{label} ({activity_type})"
    return label

def class_name_for(activity):
    if activity.when is not None:
        return "activity_with_when"
    elif activity.status == "has not happened":
        return "activity_not_happened"
    else:
        return "activity_has_happened"
