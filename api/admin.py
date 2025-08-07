from django.contrib import admin
from django.utils.html import format_html
import json

from .models import APIRequestLog


@admin.register(APIRequestLog)
class APIRequestLogAdmin(admin.ModelAdmin):
    list_display = (
        'timestamp', 'user', 'method', 'path', 'status_code',
        'duration', 'ip', 'short_query_params'
    )
    list_filter = ('method', 'status_code', 'timestamp')
    search_fields = ('path', 'user__username', 'ip')
    ordering = ('-timestamp',)
    readonly_fields = [
        'timestamp', 'user', 'method', 'path',
        'status_code', 'duration', 'ip', 'formatted_query_params'
    ]

    fieldsets = (
        (None, {
            'fields': (
                'timestamp', 'user', 'method', 'path',
                'status_code', 'duration', 'ip', 'formatted_query_params'
            )
        }),
    )

    @admin.display(description="Query Params")
    def formatted_query_params(self, obj):
        if obj.query_params:
            pretty = json.dumps(obj.query_params, indent=2, ensure_ascii=False)
            return format_html('<pre style="white-space: pre-wrap;">{}</pre>', pretty)
        return "-"

    @admin.display(description="Query Keys")
    def short_query_params(self, obj):
        # Show top-level keys of query_params in list view
        if isinstance(obj.query_params, dict):
            return ", ".join(obj.query_params.keys())
        return "-"

    def has_add_permission(self, request):
        return False  # Prevent adding new logs manually

    def has_change_permission(self, request, obj=None):
        return False  # Logs should be read-only

