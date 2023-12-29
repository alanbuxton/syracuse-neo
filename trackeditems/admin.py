from django.contrib import admin

from .models import TrackedOrganization

class TrackedOrganizationAdmin(admin.ModelAdmin):
    list_display = ["id","user","organization_name"] #TrackedOrganization._meta.get_fields()

admin.site.register(TrackedOrganization, TrackedOrganizationAdmin)
