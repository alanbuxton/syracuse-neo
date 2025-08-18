from rest_framework.test import APITestCase, APIRequestFactory, force_authenticate
from .views import *
from django.contrib.auth import get_user_model
from time import time

class TestFeedbacks(APITestCase):
    
    def setUp(self):
        factory = APIRequestFactory()
        self.request1 = factory.post("/feedbacks/v1/unprocessed", {"ids": ["1","2"]}, format='json')
        self.request2 = factory.post("/feedbacks/v1/unprocessed", {"ids": ["1","2"]}, format='json')
        self.request3 = factory.post("/feedbacks/v1/unprocessed", {"ids": [1,2]}, format='json')
        self.request4 = factory.post("/feedbacks/v1/unprocessed", {"foo":"bar"}, format='json')
        self.view = MarkAsProcessedViewSet.as_view({"post":"create"})
        ts = time()
        self.user = get_user_model().objects.create(username=f"test-{ts}")

    def test_not_available_if_not_logged_in(self):
        res = self.view(self.request1)
        self.assertEqual(res.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_handles_ids_as_text(self):
        force_authenticate(self.request2, self.user)
        res = self.view(self.request2)
        self.assertEqual(res.status_code, status.HTTP_200_OK)   
        self.assertEqual(res.data, [])
 
    def test_handles_ids_as_ints(self):
        force_authenticate(self.request3, self.user)
        res = self.view(self.request3)
        self.assertEqual(res.status_code, status.HTTP_200_OK)   
        self.assertEqual(res.data, [])

    def test_gracefully_handles_case_of_no_ids(self):
        force_authenticate(self.request4, self.user)
        res = self.view(self.request4)
        self.assertEqual(res.status_code, status.HTTP_200_OK)   
        self.assertEqual(res.data, [])

