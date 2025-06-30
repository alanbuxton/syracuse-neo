from django.test import TestCase

class CORSTest(TestCase):

    def test_cors_headers_present(self):
        response = self.client.get(
            "/api/schema/",
            HTTP_ORIGIN="http://example.com",  # Simulated cross-origin request
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Access-Control-Allow-Origin"], "*") 