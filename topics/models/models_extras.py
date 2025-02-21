'''
    Extra model definitions to ensure we don't have errors when trying to resolve node with multiple labels
'''

from neomodel import db
from neomodel.exceptions import NodeClassAlreadyDefined
import logging
from django.core.cache import cache
from .models import *
logger = logging.getLogger(__name__)

def get_multi_labels(ignore_cache=False):
    cache_key = "multi_labels_resources"
    if ignore_cache is False:
        res = cache.get(cache_key)
        if res is not None:
            return res
    query = "match (n: Resource) where size(labels(n)) > 2 return distinct labels(n)"
    labels_arr, _ = db.cypher_query(query)
    res = [x[0] for x in labels_arr]
    cache.set(cache_key, res)
    return res

def class_factory(name, base): # ChatGPT!
    attrs = {"__class_name_is_label__":False}
    cls = type(name, base, attrs)
    cls.__module__ = "topics.models.models_extras" 
    globals()[name] = cls  
    return cls

def add_dynamic_classes_for_multiple_labels(ignore_cache=False):
    classes = []
    labels_list = get_multi_labels(ignore_cache=ignore_cache)
    for labels in labels_list:
        assert "Resource" in labels, f"Expected {labels} to include 'Resource'"
        labels.remove("Resource")
        class_name = "".join(labels)
        parent_classes = tuple([globals()[x] for x in labels])
        logger.debug(f"Defining {class_name}")
        try:
            new_class = class_factory(class_name,parent_classes)
            classes.append(new_class)
        except NodeClassAlreadyDefined:
            logger.debug(f"{class_name} was already defined")
    return classes

_ = add_dynamic_classes_for_multiple_labels()
