from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView

class NoThrottleMixin:
    def get_throttles(self):
        return []

class NoThrottleSpectacularAPIView(NoThrottleMixin, SpectacularAPIView):
    pass

class NoThrottleSpectacularRedocView(NoThrottleMixin, SpectacularRedocView):
    pass

class NoThrottleSpectacularSwaggerView(NoThrottleMixin, SpectacularSwaggerView):
    pass