from django.contrib import admin

from .models import DataField, ExtractionInstance, ExtractionJob, ExtractionTask, FilterField


class FilterFieldInline(admin.StackedInline):
    model = FilterField
    extra = 1
    ordering = ("id",)


class DataFieldInline(admin.StackedInline):
    model = DataField
    extra = 1
    ordering = ("id",)


class ExtractionJobAdmin(admin.ModelAdmin):
    inlines = [FilterFieldInline, DataFieldInline]


admin.site.register(ExtractionJob, ExtractionJobAdmin)


class ExtractionInstanceInline(admin.StackedInline):
    model = ExtractionInstance
    extra = 1
    ordering = ("id",)


class ExtractionTaskAdmin(admin.ModelAdmin):
    inlines = [ExtractionInstanceInline]


admin.site.register(ExtractionTask, ExtractionTaskAdmin)
