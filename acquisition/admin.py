from django.contrib import admin
from .models import HISPoint, PointType, ScadaUnitConfig, SmartHIS


@admin.register(SmartHIS)
class SmartHISAdmin(admin.ModelAdmin):
    list_display = ('farm', 'username', 'address', 'created_at')
    list_filter = ('created_at',)
    search_fields = ('farm__name', 'username', 'address')
    readonly_fields = ('created_at',)
    raw_id_fields = ('farm',)


@admin.register(PointType)
class PointTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'key', 'level', 'column_name')
    list_filter = ('level',)
    search_fields = ('name', 'key', 'column_name')


@admin.register(HISPoint)
class HISPointAdmin(admin.ModelAdmin):
    list_display = ('point_name', 'farm', 'point_type', 'turbine', 'is_active', 'created_at')
    list_filter = ('farm', 'point_type', 'turbine', 'is_active', 'created_at')
    search_fields = ('point_name', 'farm__name', 'turbine__name', 'point_type__name')
    readonly_fields = ('created_at', 'updated_at')
    raw_id_fields = ('farm', 'point_type', 'turbine')


@admin.register(ScadaUnitConfig)
class ScadaUnitConfigAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "data_source",
        "farm",
        "turbine",
        "active_power_unit",
        "wind_speed_unit",
        "temperature_unit",
        "pressure_unit",
        "humidity_unit",
        "updated_at",
    )
    list_filter = (
        "data_source",
        "farm",
        "turbine",
        "active_power_unit",
        "wind_speed_unit",
        "temperature_unit",
        "pressure_unit",
        "humidity_unit",
        "updated_at",
    )
    search_fields = ("id", "farm__name", "turbine__name")
    readonly_fields = ("created_at", "updated_at")
    raw_id_fields = ("farm", "turbine")
    ordering = ("-updated_at", "-created_at")
    fieldsets = (
        (
            "Scope",
            {"fields": ("data_source", "farm", "turbine")},
        ),
        (
            "Raw units",
            {
                "fields": (
                    "active_power_unit",
                    "wind_speed_unit",
                    "temperature_unit",
                    "pressure_unit",
                    "humidity_unit",
                )
            },
        ),
        (
            "OEM scaling (optional multipliers)",
            {
                "fields": (
                    "active_power_multiplier",
                    "wind_speed_multiplier",
                    "temperature_multiplier",
                    "pressure_multiplier",
                    "humidity_multiplier",
                )
            },
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )
