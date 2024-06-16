from rest_framework import serializers
from .graph_utils import graph_centered_on
from .converters import CustomSerializer
from .timeline_utils import get_timeline_data
from .geo_utils import geo_select_list
from .industry_utils import industry_select_list
from .model_queries import org_family_tree

class OrganizationSerializer(serializers.ModelSerializer):

    def to_representation(self,instance):
        repres = instance.serialize()
        splitted_uri = instance.split_uri()
        repres["splitted_uri"] = splitted_uri
        return repres

class ResourceSerializer(serializers.Serializer):
    def to_representation(self, instance):
        return instance.serialize_no_none()

class FamilyTreeSerializer(serializers.BaseSerializer):

    def generate_edge_data(self, activity, article, label, docExtract):
        return {
            "activity_type": label,
            "source_organization": article.sourceOrganization,
            "date_published": article.datePublished,
            "headline": article.headline,
            "document_extract": docExtract,
            "activity_status": activity.status_as_string,
            "document_url": article.documentURL,
            "internal_archive_page_url": article.archiveOrgPageURL,
            "internet_archive_list_url": article.archiveOrgListURL,
            "activity_uri": activity.uri,
            "article_uri": article.uri,
        }

    def to_representation(self, instance):
        organization_uri = instance.uri
        include_same_as_name_only = self.context.get('include_same_as_name_only',True)
        parents, self_and_siblings, children = org_family_tree(organization_uri, include_same_as_name_only=include_same_as_name_only)
        nodes = []
        edges = []
        node_data = {}
        edge_data = {}

        for parent,_,_,_,_ in parents:
            node_data[parent.uri] = parent.serialize_no_none()
            nodes.append( {"id": parent.uri, "label": parent.best_name, "level": 0})
            for level1,activity,art,label,docExtract in self_and_siblings:
                edge_data[f"{parent.uri }-{label}-{level1.uri}"] = self.generate_edge_data(activity,art,label,docExtract)
                edge_label = f"{label.title()} ({art.sourceOrganization} {art.datePublished.strftime('%b %Y')})"
                edges.append ( { "id": f"{ parent.uri }-{ label }-{ level1.uri }",
                            "from": f"{ parent.uri }", "to": f"{ level1.uri }", "color": "blue", "label": edge_label, "arrows": "to" })

        for level1,_,_,_,_ in self_and_siblings:
            node_data[level1.uri] = level1.serialize_no_none()
            node_attribs = {"id": level1.uri, "label": level1.best_name, "level": 1}
            if level1.uri == organization_uri:
                node_attribs['color'] = '#f6c655'
            nodes.append(node_attribs)

        for level2,activity,art,label,docExtract in children:
            node_data[level2.uri] = level2.serialize_no_none()
            node_attribs = {"id": level2.uri, "label": level2.best_name, "level": 2}
            nodes.append(node_attribs)
            edge_data[f"{organization_uri }-{label}-{level2.uri}"] =  self.generate_edge_data(activity,art,label,docExtract)
            edge_label = f"{label.title()} ({art.sourceOrganization} {art.datePublished.strftime('%b %Y')})"
            edges.append( { "id": f"{ organization_uri }-{ label }-{ level2.uri }",
                        "from": f"{ organization_uri }", "to": f"{ level2.uri }", "color": "blue", "label": edge_label, "arrows": "to" })

        return {
            "nodes": nodes,
            "node_details": CustomSerializer(node_data),
            "edges": edges,
            "edge_details": CustomSerializer(edge_data),
        }



class OrganizationGraphSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        graph_data = graph_centered_on(instance,**self.context)
        data = {"source_node": instance.uri,
                "source_node_name": instance.longest_name,
                "too_many_nodes":False}
        if graph_data is None:
            data["node_data"] = []
            data["edge_data"] = []
            data["too_many_nodes"] = True
            return data
        clean_node_data, clean_edge_data, node_details, edge_details = graph_data
        data["node_data"] = clean_node_data
        data["edge_data"] = clean_edge_data
        data["node_details"] = CustomSerializer(node_details)
        data["edge_details"] = CustomSerializer(edge_details)
        nodes_by_type = {}
        for node_row in clean_node_data:
            node_type = node_row['entity_type']
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node_row['id'])
        data["nodes_by_type"] = nodes_by_type
        return data

class NameSearchSerializer(serializers.Serializer):
    name = serializers.CharField(
        max_length=20,
        style={'placeholder': 'Search ...', 'autofocus': True}
    )

class DataListChoiceField(serializers.ChoiceField):

    def __init__(self, choices, **kwargs):
        super().__init__(choices, **kwargs)
        self.choices = [x for _,x in choices]
        self.style = {"base_template": "datalist-select.html"}
        self.text_to_id = {v:k for k,v in choices}

    def to_internal_value(self, data):
        ''' data list allows for any text to be entered,
            so provided text won't always match '''
        res = self.text_to_id.get(data,None)
        return res

class IndustrySerializer(serializers.Serializer):
    industry =  DataListChoiceField(choices=industry_select_list())

    def get_industry_id(self):
        self.is_valid()
        return self['industry'].value

class GeoSerializer(serializers.Serializer):
    country_or_region = DataListChoiceField(choices=geo_select_list(include_alt_names=True))

    def get_country_or_region_id(self):
        self.is_valid()
        return self['country_or_region'].value

class TimelineSerializer(serializers.Serializer):
    def to_representation(self, instance, **kwargs):
        limit = 10
        groups, items, item_display_details, org_display_details, errors = get_timeline_data(instance, limit)
        errors = sorted(errors)
        if len(errors) > 50:
            errors = "; ".join(errors[:50]) + f" plus {len(errors) - 50} other organizations"
        else:
            errors = "; ".join(errors)
        if len(groups) + len(errors) > limit:
            limit_message = f'Max {limit} organizations shown for web users. Please <a href="mailto:info-syracuse@1145.am?subject=Want%20to%20see%20more%20Syracuse%20data&body=Dear%20Info%0D%0AI%20would%20like%20to%20discuss%20accessing%20timeline%20data.">contact us</a> for API or bulk data.'
        else:
            limit_message = ""
        resp = {"groups": groups, "items":items,
            "item_display_details":CustomSerializer(item_display_details),
            "org_display_details": CustomSerializer(org_display_details),
            "errors": errors,
            "limit_msg": limit_message,
            }
        return resp

class OrganizationTimelineSerializer(serializers.BaseSerializer):

    def to_representation(self, instance, **kwargs):
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([instance], None)
        errors = sorted(errors)
        if len(errors) > 50:
            errors = "; ".join(errors[:50]) + f" plus {len(errors) - 50} other organizations"
        else:
            errors = "; ".join(errors)
        resp = {"groups": groups, "items":items,
            "item_display_details":CustomSerializer(item_display_details),
            "org_name": instance.longest_name,
            "org_node": instance.uri,
            "org_display_details": CustomSerializer(org_display_details),
            "errors": errors,
            }
        return resp
