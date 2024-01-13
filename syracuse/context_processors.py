from django.conf import settings

def set_constants(request):
    return {
        "USE_GOOGLE_ANALYTICS": settings.USE_GOOGLE_ANALYTICS,
    }
