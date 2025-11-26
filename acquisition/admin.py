from django.contrib import admin
from .models import SmartHIS, PointType, HISPoint


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
