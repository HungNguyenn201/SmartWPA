from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import Account, License


@admin.register(Account)
class AccountAdmin(BaseUserAdmin):
    list_display = ('username', 'email', 'role', 'investor_profile', 'farm', 'is_active', 'is_staff', 'date_created')
    list_filter = ('role', 'is_active', 'is_staff', 'is_superuser', 'date_created')
    search_fields = ('username', 'email')
    readonly_fields = ('date_created', 'last_login')
    
    fieldsets = (
        (None, {'fields': ('username', 'password')}),
        ('Personal info', {'fields': ('email', 'role', 'investor_profile', 'farm', 'manager')}),
        ('Permissions', {'fields': ('is_active', 'is_staff', 'is_superuser', 'groups', 'user_permissions')}),
        ('Important dates', {'fields': ('last_login', 'date_created')}),
    )
    
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('username', 'email', 'password1', 'password2', 'role'),
        }),
    )
    
    raw_id_fields = ('investor_profile', 'farm', 'manager')


@admin.register(License)
class LicenseAdmin(admin.ModelAdmin):
    list_display = ('investor', 'key', 'is_permanent', 'expiry_date', 'created_at', 'is_valid')
    list_filter = ('is_permanent', 'created_at', 'expiry_date')
    search_fields = ('investor__name', 'investor__email', 'key')
    readonly_fields = ('created_at', 'is_valid')
    raw_id_fields = ('investor',)
    
    def is_valid(self, obj):
        if obj:
            return obj.is_valid()
        return False
    is_valid.boolean = True
    is_valid.short_description = 'Valid'
