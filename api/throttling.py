from rest_framework.throttling import UserRateThrottle
from allauth.account.models import EmailAddress
from django.conf import settings
from logging import getLogger
logger = getLogger(__name__)

class ScopedTieredThrottle(UserRateThrottle):
    scope = 'default_scope'

    def allow_request(self, request, view):
        if "/api/" not in request.path:
            return True
        if not request.user.is_authenticated:
            self.rate = "1/month"
        else:
            user = request.user
            if EmailAddress.objects.filter(user=user, verified=True).exists():
                self.rate = f"{settings.THROTTLES['verified_user']}/month"
            else:
                self.rate = f"{settings.THROTTLES['unverified_user']}/month"
        self.num_requests, self.duration = self.parse_rate(self.rate)
        logger.debug(f"[Throttle] Authenticated: {request.user.is_authenticated}")
        logger.debug(f"[Throttle] User: {request.user}")
        logger.debug(f"[Throttle] View: {view.__module__}")
        logger.debug(f"[Throttle] {self.num_requests} {self.duration}")
        logger.debug(f"[Throttle] {self.get_cache_key(request, view)}")
        return super().allow_request(request, view)

    def get_cache_key(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return None
        ident = request.user.pk
        return self.cache_format % {
            'scope': self.scope,
            'ident': ident
        }
    
    def parse_rate(self, rate):
        if rate is None:
            return None, None

        num, period = rate.split('/')
        num_requests = int(num)

        # Handle 'month' manually
        if period == 'month':
            duration = 60 * 60 * 24 * 30  # 30 days
        elif period == 'day':
            duration = 60 * 60 * 24
        elif period == 'hour':
            duration = 60 * 60
        elif period == 'minute':
            duration = 60
        elif period == 'second':
            duration = 1
        else:
            raise ValueError(f"Invalid throttle period: {period}")

        return num_requests, duration

