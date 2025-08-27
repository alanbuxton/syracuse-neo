# accounts/admin.py
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import UserProfile
from rest_framework.authtoken.models import Token

User = get_user_model()

class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    verbose_name_plural = "Profile"

class UserAdmin(BaseUserAdmin):
    inlines = (UserProfileInline,)

    # Add profile field to list_display
    def monthly_api_limit(self, obj):
        return obj.userprofile.monthly_api_limit
    monthly_api_limit.admin_order_field = "userprofile__monthly_api_limit"
    monthly_api_limit.short_description = "Monthly API Limit"

    def api_token(self, obj):
        try:
            token = Token.objects.get(user=obj)
            return token.key
        except Token.DoesNotExist:
            return "-"
    api_token.short_description = "API Token"

    list_display = (
        "username", "email", "api_token", "monthly_api_limit",  
        "first_name", "last_name", "is_staff", "is_superuser", "is_active", "last_login",
    )


admin.site.unregister(User)
admin.site.register(User, UserAdmin)