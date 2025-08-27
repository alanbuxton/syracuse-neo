from django.apps import AppConfig


class AuthExtensionsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'auth_extensions'

    def ready(self):
        import auth_extensions.signals
