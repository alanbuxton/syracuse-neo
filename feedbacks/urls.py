from django.urls import path, include
from rest_framework.routers import DefaultRouter
from feedbacks import views

# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r"create", views.InteractiveFeedbackViewSet, basename='interactive-feedbacks')
router.register(r"v1/unprocessed", views.MarkAsProcessedViewSet, basename='unprocessed-feedbacks')


# The API URLs are now determined automatically by the router.
urlpatterns = [
    path('', include(router.urls)),
]
