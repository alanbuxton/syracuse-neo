from django.contrib import admin

# Register your models here.
from .models import Feedback

class FeedbackAdmin(admin.ModelAdmin):
    list_display = list(map(lambda x: x.name ,Feedback._meta.fields))

admin.site.register(Feedback, FeedbackAdmin)
