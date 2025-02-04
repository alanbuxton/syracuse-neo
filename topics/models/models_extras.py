'''
    Extra model definitions to ensure we don't have errors when trying to resolve node with multiple labels
'''

from neomodel import db
from neomodel.exceptions import NodeClassAlreadyDefined
import logging
from django.core.cache import cache
from .models import *
logger = logging.getLogger(__name__)

def get_multi_labels():
    cache_key = "multi_labels_resources"
    res = cache.get(cache_key)
    if res is not None:
        return res
    query = "match (n: Resource) where size(labels(n)) > 2 return distinct labels(n)"
    labels_arr, _ = db.cypher_query(query)
    res = [x[0] for x in labels_arr]
    cache.set(cache_key, res)
    return res

def add_dynamic_classes_for_multiple_labels():
    classes = []
    labels_list = get_multi_labels()
    for labels in labels_list:
        assert "Resource" in labels, f"Expected {labels} to include 'Resource'"
        labels.remove("Resource")
        class_name = "".join(labels)
        parent_classes = tuple([globals()[x] for x in labels])
        logger.info(f"Defining {class_name}")
        try:
            new_class = type(class_name,tuple(parent_classes),{"__class_name_is_label__":False})
            classes.append(new_class)
        except NodeClassAlreadyDefined:
            logger.info(f"{class_name} was already defined")
    return classes

_ = add_dynamic_classes_for_multiple_labels()