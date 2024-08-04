from django import template
from django.utils.html import escape, mark_safe
from django.urls import reverse
import re
from urllib.parse import urlencode


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
    if len(request.GET) > 0:
        link_uri = f"{link_uri}?{urlencode(request.GET)}"
    return f"<a href='{link_uri}'>{display_uri}</a>"

def local_uri(uri, request):
    return re.sub(r"^https://",f"{request.scheme}://{request.get_host()}/resource/",uri)

def prettify_snake_case(text):
    return text.replace("_"," ").title()

def prettify_camel_case(text):
    # https://stackoverflow.com/a/37697078/7414500
    text = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', text))
    return text.title()
    

def dict_to_query_string(data):
    if data is None or data == []:
        return ''
    return mark_safe(urlencode(data))


@register.simple_tag
def url_with_querystring(viewname, *args, qs_params=[]):
    # Add your custom logic here
    url = reverse(viewname, args=args)
    # For example, you could add a query parameter to all URLs:
    if qs_params is not None and len(qs_params) > 0:
        return mark_safe(f"{url}?{ urlencode(qs_params) }")
    else:
        return mark_safe(url)

register.filter("pretty_print_list_uri",pretty_print_list_uri)
register.filter("local_uri",local_uri)
register.filter("prettify_snake_case",prettify_snake_case)
register.filter("prettify_camel_case",prettify_camel_case)
register.filter("dict_to_query_string",dict_to_query_string)
