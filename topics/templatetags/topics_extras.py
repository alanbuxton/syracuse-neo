from django import template
from django.utils.html import escape, mark_safe
import re

register = template.Library()

def pretty_print_list_uri(value, request):
    if not isinstance(value, list):
        value = [value]
    results = []
    for val in value:
        if val is None:
            continue
        elif not isinstance(val,str):
            res = str(val)
        elif re.search(r"^https?://", val):
            res = pretty_local_uri(val, request)
        else:
            res = escape(val)
        results.append(res)
    return mark_safe(", ".join(results))

def pretty_local_uri(uri, request):
    display_uri = uri
    if uri.startswith("https://1145.am/db"):
        link_uri = local_uri(uri, request)
    else:
        link_uri = uri
    return f"<a href='{link_uri}'>{display_uri}</a>"

def local_uri(uri, request):
    return re.sub(r"^https://",f"{request.scheme}://{request.get_host()}/resource/",uri)


register.filter("pretty_print_list_uri",pretty_print_list_uri)
register.filter("local_uri",local_uri)
