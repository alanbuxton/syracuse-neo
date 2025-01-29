from django.contrib import admin

from .models import TrackedOrganization, TrackedIndustryGeo, TrackedItem

class TrackedOrganizationAdmin(admin.ModelAdmin):
    list_display = ["id","user","organization_uri"]

class TrackedIndustryGeoAdmin(admin.ModelAdmin):
    list_display = ["id", "user", "industry_name","geo_code"]


admin.site.register(TrackedOrganization, TrackedOrganizationAdmin)
admin.site.register(TrackedIndustryGeo, TrackedIndustryGeoAdmin)

admin.site.register(TrackedItem)
