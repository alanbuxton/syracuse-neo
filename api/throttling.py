from rest_framework.throttling import UserRateThrottle
from allauth.account.models import EmailAddress
from django.conf import settings

class ScopedTieredThrottle(UserRateThrottle):
    scope = 'api'
    
    def allow_request(self, request, view):
        if not request.user.is_authenticated:
            self.rate = "1/day"
        else:
            user = request.user
            if EmailAddress.objects.filter(user=user, verified=True).exists():
                self.rate = f"{settings.THROTTLES['verified_user']}/month"
            else:
                self.rate = f"{settings.THROTTLES['unverified_user']}/month"
        return super().allow_request(request, view)
