from django.contrib import admin
from .models import Investor, Farm, Turbines


@admin.register(Investor)
class InvestorAdmin(admin.ModelAdmin):
    list_display = ('name', 'email', 'is_active', 'created_at')
    list_filter = ('is_active', 'created_at')
    search_fields = ('name', 'email')
    readonly_fields = ('created_at',)


@admin.register(Farm)
class FarmAdmin(admin.ModelAdmin):
    list_display = ('name', 'investor', 'capacity', 'address', 'time_created')
    list_filter = ('investor', 'time_created')
    search_fields = ('name', 'address')
    readonly_fields = ('time_created',)
    raw_id_fields = ('investor',)


@admin.register(Turbines)
class TurbinesAdmin(admin.ModelAdmin):
    list_display = ('name', 'farm', 'capacity', 'is_active', 'time_created')
    list_filter = ('farm', 'is_active', 'time_created')
    search_fields = ('name', 'farm__name')
    readonly_fields = ('time_created', 'last_data_update')
    raw_id_fields = ('farm',)
