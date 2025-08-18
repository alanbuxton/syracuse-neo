from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase, APIClient
from unittest.mock import patch
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token
from django.test import TestCase, override_settings
from datetime import datetime, timezone

@override_settings(ROOT_URLCONF='api.tests.auth_urls') 
class AuthenticationAuthorizationTestCase(TestCase):
    def setUp(self):
        self.client = APIClient()
        
        # Create test users
        self.regular_user = User.objects.create_user(
            username='regular_user',
            password='testpass123'
        )
        self.admin_user = User.objects.create_user(
            username='admin_user',
            password='testpass123',
            is_staff=True,
            is_superuser=True
        )
        
        # Create tokens
        self.regular_token = Token.objects.create(user=self.regular_user)
        self.admin_token = Token.objects.create(user=self.admin_user)
        self.invalid_token = "invalid_token_12345"

    def test_no_token_returns_401(self):
        """Test that missing authentication returns 401"""
        response = self.client.get('/api/protected/')
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn('Authentication credentials were not provided', 
                     str(response.data.get('detail', '')))

    def test_invalid_token_returns_401(self):
        """Test that invalid token returns 401"""
        self.client.credentials(HTTP_AUTHORIZATION=f'Token {self.invalid_token}')
        response = self.client.get('/api/protected/')
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_valid_token_wrong_permissions_returns_403(self):
        """Test that valid authentication but insufficient permissions returns 403"""
        # Use regular user token to access admin-only endpoint
        self.client.credentials(HTTP_AUTHORIZATION=f'Token {self.regular_token.key}')
        response = self.client.get('/api/admin-only/')
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        # Should contain permission-related message, not authentication
        self.assertNotIn('Authentication', str(response.data.get('detail', '')))

    def test_valid_token_sufficient_permissions_returns_200(self):
        """Test that valid authentication with sufficient permissions works"""
        # Use regular user token for regular protected endpoint
        self.client.credentials(HTTP_AUTHORIZATION=f'Token {self.regular_token.key}')
        response = self.client.get('/api/protected/')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_admin_token_admin_endpoint_returns_200(self):
        """Test that admin user can access admin endpoints"""
        self.client.credentials(HTTP_AUTHORIZATION=f'Token {self.admin_token.key}')
        response = self.client.get('/api/admin-only/')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_malformed_auth_header_returns_401(self):
        """Test that malformed authorization header returns 401"""
        self.client.credentials(HTTP_AUTHORIZATION='Bearer invalid_format')
        response = self.client.get('/api/protected/')
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_empty_token_returns_401(self):
        """Test that empty token returns 401"""
        self.client.credentials(HTTP_AUTHORIZATION='Token ')
        response = self.client.get('/api/protected/')
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_multiple_auth_scenarios(self):
        """Test multiple scenarios in sequence"""
        scenarios = [
            # (auth_header, endpoint, expected_status, description)
            (None, '/api/protected/', 401, 'No auth header'),
            (f'Token {self.invalid_token}', '/api/protected/', 401, 'Invalid token'),
            (f'Token {self.regular_token.key}', '/api/protected/', 200, 'Valid user, allowed endpoint'),
            (f'Token {self.regular_token.key}', '/api/admin-only/', 403, 'Valid user, forbidden endpoint'),
            (f'Token {self.admin_token.key}', '/api/admin-only/', 200, 'Admin user, admin endpoint'),
            (f'Token {self.admin_token.key}', '/api/protected/', 200, 'Admin user, regular endpoint'),
        ]
        
        for auth_header, endpoint, expected_status, description in scenarios:
            with self.subTest(description=description):
                self.client.credentials()  # Clear credentials
                
                if auth_header:
                    self.client.credentials(HTTP_AUTHORIZATION=auth_header)
                
                response = self.client.get(endpoint)
                self.assertEqual(
                    response.status_code, 
                    expected_status,
                    f"{description}: Expected {expected_status}, got {response.status_code}"
                )

@override_settings(ROOT_URLCONF='api.tests.auth_urls') 
class CustomExceptionHandlerTestCase(TestCase):
    """Additional tests specifically for the custom exception handler logic"""
    
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(username='testuser', password='pass')
        self.token = Token.objects.create(user=self.user)

    def test_authenticated_user_permission_denied_stays_403(self):
        """Verify that PermissionDenied with authenticated user stays 403"""
        self.client.credentials(HTTP_AUTHORIZATION=f'Token {self.token.key}')
        
        # Hit an endpoint that the user is authenticated for but not authorized
        response = self.client.get('/api/admin-only/')
        
        # Should be 403, not 401, because user IS authenticated
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_anonymous_user_gets_401(self):
        """Verify that anonymous users get 401 for comparison"""
        # Don't set credentials - user will be anonymous
        response = self.client.get('/api/admin-only/')
        
        # Should be 401 for anonymous users
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        
        # Verify user is actually anonymous
        self.assertFalse(response.wsgi_request.user.is_authenticated)
        
    def test_invalid_token_gets_401(self):
        """Verify that invalid tokens result in 401"""
        self.client.credentials(HTTP_AUTHORIZATION='Token invalid_token_here')
        response = self.client.get('/api/admin-only/')
        
        # Should be 401 for invalid token
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        
        # User should still be anonymous due to invalid token
        self.assertFalse(response.wsgi_request.user.is_authenticated)


class BaseAuthenticatedAPITestCase(APITestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(
            username="testuser", password="pass123"
        )
        self.client.force_authenticate(user=self.user)


# ---------- IndustryClusterViewSet ----------

class IndustryClusterViewSetTests(BaseAuthenticatedAPITestCase):
    def test_industrycluster_not_found_returns_404(self):
        url = reverse("v1:api-industrycluster-detail", args=["999999"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_industrycluster_requires_authentication(self):
        self.client.force_authenticate(user=None)  # remove auth
        url = reverse("v1:api-industrycluster-detail", args=["999999"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)


# ---------- GeoNamesViewSet ----------

class GeoNamesViewSetTests(BaseAuthenticatedAPITestCase):
    def test_geonames_invalid_pk_returns_404(self):
        url = reverse("v1:api-geonameslocation-detail", args=["not-a-number"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_geonames_not_found_returns_404(self):
        url = reverse("v1:api-geonameslocation-detail", args=["123456"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_geonames_requires_authentication(self):
        self.client.force_authenticate(user=None)
        url = reverse("v1:api-geonameslocation-detail", args=["123456"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)


# ---------- RegionsViewSet ----------

class RegionsViewSetTests(BaseAuthenticatedAPITestCase):
    def test_regions_not_found_returns_404(self):
        url = reverse("v1:api-region-detail", args=["does-not-exist"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_regions_requires_authentication(self):
        self.client.force_authenticate(user=None)
        url = reverse("v1:api-region-detail", args=["does-not-exist"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    @patch("api.views.geo_parent_children")
    def test_regions_happy_path_returns_200(self, mock_geo):
        mock_geo.return_value = {'US': {'parent': 'Northern America', 'id': 'US', 'children': {'Northeast', 'South', 'West', 'Midwest'}}}
        url = reverse("v1:api-region-detail", args=["US"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], "US")
        self.assertEqual(len(response.data['children']), 4)


# ---------- ActivitiesViewSet ----------

class ActivitiesViewSetTests(BaseAuthenticatedAPITestCase):

    @patch("api.views.min_and_max_date")
    def test_activities_missing_required_params_returns_400(self, mock_min_max_date):
        mock_min_max_date.return_value = (datetime(2025,1,1,0,0,0,tzinfo=timezone.utc), 
                                          datetime(2025,5,31,23,59,59,tzinfo=timezone.utc))
        url = reverse("v1:api-activity-list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_activities_requires_authentication(self):
        self.client.force_authenticate(user=None)
        url = reverse("v1:api-activity-list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    @patch("api.views.min_and_max_date")
    @patch("api.views.get_activities_by_org_uris_and_date_range")
    def test_activities_happy_path_returns_200(self, mock_get_acts, mock_min_max_dates):
        mock_get_acts.return_value = [
            {"date_published": "2025-01-01", "activity_class": "Event",
             "headline": "foo", "source_organization": "bar", "document_extract": "baz",
             "document_url": "http://example.org/1", "activity_uri": "http://example.org/2",
             "activity_locations": [], "actors": {}, "archive_org_list_url": "http://example.org/3"}
        ]
        mock_min_max_dates.return_value = (datetime(2025,1,1,0,0,0,tzinfo=timezone.utc), 
                                          datetime(2025,5,31,23,59,59,tzinfo=timezone.utc))
        url = reverse("v1:api-activity-list") + "?org_uri=org:1"
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['results'][0]["activity_uri"], "http://example.org/2")
