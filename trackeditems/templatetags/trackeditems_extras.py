from datetime import datetime
from django import template

register = template.Library()

def iso_format_to_date(value):
    return datetime.fromisoformat(value)

register.filter(iso_format_to_date)
