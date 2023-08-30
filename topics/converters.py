import json
from datetime import datetime, date

class DateConverter:
    regex = '\d{4}-\d{1,2}-\d{1,2}'
    format = '%Y-%m-%d'

    def to_python(self, value):
        return datetime.strptime(value, self.format).date()

    def to_url(self, value):
        return value.strftime(self.format)


# https://stackoverflow.com/a/27058505/7414500
class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, set):
            return ", ".join(o)
        if isinstance(o, list):
            return ", ".join(o)
        if isinstance(o, dict):
            return list(o.items())

        return super().default(o)

def CustomSerializer(x):
    return json.dumps(x,cls=CustomEncoder)
