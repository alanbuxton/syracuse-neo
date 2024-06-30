from django.test import TestCase
from datetime import datetime, timezone
from .models import P

class PrecalculatedDataTestCase(TestCase):

    def setUpTestData():
        P.nuke_all()
        assert len(P.objects.all()) == 0

    def test_stores_last_updated(self):
        ts = datetime.now(tz=timezone.utc)
        v = P.get_last_updated()
        assert v is None
        v2 = P.set_last_updated(ts)
        assert v2 == ts
        assert len(P.objects.all()) == 1
