import time
from django.utils.timezone import now
from api.models import APIRequestLog
from django.conf import settings
from django.urls import resolve, Resolver404

class APIUsageMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start_time = time.time()
        response = self.get_response(request)
        duration = time.time() - start_time

        user = getattr(request, 'user', None)
        path = request.path

        # Log if it's an authenticated API call
        if user and user.is_authenticated and '/api/v' in path:

            # Skip logging if it's a trailing slash 404
            if (
                response.status_code == 404 and
                settings.APPEND_SLASH and
                request.method in ("GET", "HEAD") and
                not path.endswith('/')
            ):
                try:
                    resolve(path + '/')
                    # This is a trailing-slash-related 404, so skip logging
                    return response
                except Resolver404:
                    pass  # Not a slash issue; proceed to log

            ip = request.META.get('REMOTE_ADDR', '')
            
            # Convert query params to JSON-safe dict
            query_params = {
                k: v if len(v) > 1 else v[0]
                for k, v in dict(request.GET.lists()).items()
                if k.lower() not in ['token', 'password', 'secret']
            }

            APIRequestLog.objects.create(
                user=user,
                path=path,
                method=request.method,
                status_code=response.status_code,
                duration=round(duration, 4),
                ip=ip,
                query_params=query_params,
                timestamp=now()
            )

        return response