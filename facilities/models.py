from django.db import models
from datetime import datetime
import uuid
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

class Investor(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=100, unique=True)
    email = models.EmailField(unique=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def clean(self):
        if not self.name:
            raise ValidationError(_('Name is required'))
        if not self.email:
            raise ValidationError(_('Email is required'))
        if self.email and not '@' in self.email:
            raise ValidationError(_('Invalid email format'))

    def generate_license(self, is_permanent=True, expiry_date=None):
        """Tạo License model cho investor"""
        from permissions.models import License
        
        # Tạo key dạng UUID4
        license_key = str(uuid.uuid4())
        
        # Tạo hoặc cập nhật License
        license_obj, created = License.objects.get_or_create(
            investor=self,
            defaults={
                'key': license_key,
                'is_permanent': is_permanent,
                'expiry_date': expiry_date,
            }
        )
        return license_obj
        
    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ('name',)

class Farm(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=50, unique=True)
    address = models.CharField(max_length=100, blank=True, null=True)   
    capacity = models.FloatField(null=True)
    latitude = models.FloatField(null=True)
    longitude = models.FloatField(null=True)
    investor = models.ForeignKey(Investor, on_delete=models.CASCADE, related_name="farms", null=True, blank=True)
    time_created = models.DateTimeField()
    
    def clean(self):
        if not self.name:
            raise ValidationError(_('Name is required'))
        if self.capacity is not None and self.capacity <= 0:
            raise ValidationError(_('Capacity must be positive'))
    
    def save(self, *args, **kwargs):
        if not self.time_created:
            self.time_created = datetime.now()
        self.clean()
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f'{self.name}'
    
    class Meta:
        ordering = ('name',)

class Turbines(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=50, unique=True)
    farm = models.ForeignKey(Farm, on_delete=models.CASCADE, related_name="turbines")
    latitude = models.FloatField(null=True)
    longitude = models.FloatField(null=True)
    capacity = models.FloatField(null=True)
    time_created = models.DateTimeField()
    is_active = models.BooleanField(default=True)
    last_data_update = models.DateTimeField(null=True, blank=True)
    
    def clean(self):
        if not self.name:
            raise ValidationError(_('Name is required'))
        if not self.farm:
            raise ValidationError(_('Farm is required'))
        if self.capacity is not None and self.capacity <= 0:
            raise ValidationError(_('Capacity must be positive'))
    
    def save(self, *args, **kwargs):
        if not self.time_created:
            self.time_created = datetime.now()
        self.clean()
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f'{self.name}'
    
    class Meta:
        ordering = ('name',)


