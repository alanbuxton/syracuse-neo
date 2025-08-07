import time
from django.utils.timezone import now
from api.models import APIRequestLog

class APIUsageMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start_time = time.time()
        response = self.get_response(request)
        duration = time.time() - start_time

        user = getattr(request, 'user', None)

        # Only log authenticated users (optional)
        if user and user.is_authenticated:
            path = request.path
            ip = request.META.get('REMOTE_ADDR', '')

            # Convert query dict to flat dict: {key: single_value}
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
