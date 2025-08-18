from rest_framework.authentication import TokenAuthentication
import re
from rest_framework.views import exception_handler
from rest_framework import status

class FlexibleTokenAuthentication(TokenAuthentication):
    def authenticate(self, request):
        auth = request.META.get("HTTP_AUTHORIZATION", "")
        if auth.lower().startswith("bearer "):
            reg = re.compile(r"^bearer ",re.IGNORECASE)
            request.META['HTTP_AUTHORIZATION'] = reg.sub("Token ", auth) 
        if len(auth) > 0: 
            request._auth_attempted = True
        return super().authenticate(request)


def custom_exception_handler(exc, context):
    '''
    DRF usually returns a 403 for auth failures, but a 401 is more accurate
    403 is "forbidden" - so I've provided valid login credentials but access was denied.
    401 is "unauthorized" - I didn't provide valid login credentials
    '''
    response = exception_handler(exc, context)
    
    if response is not None:
        # Convert authentication-related 403s to 401
        if response.status_code == status.HTTP_403_FORBIDDEN:

            request = context.get('request')

            if not hasattr(request, "_auth_attempted"):
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data = {'detail': 'Invalid authentication credentials. Please check your token and ensure it follows the format: "Authorization: Token your_token_here"'}
                return response
                
    return response

