from rest_framework.authentication import TokenAuthentication
import re

class FlexibleTokenAuthentication(TokenAuthentication):
    def authenticate(self, request):
        auth = request.META.get("HTTP_AUTHORIZATION", "")
        if auth.lower().startswith("bearer "):
            reg = re.compile(r"^bearer ",re.IGNORECASE)
            request.META['HTTP_AUTHORIZATION'] = reg.sub("Token ", auth)
        return super().authenticate(request)
    
