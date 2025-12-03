from django.db import models
from facilities.models import Farm, Turbines

# Create your models here.
class SmartHIS(models.Model):
    objects = models.Manager()
    farm = models.OneToOneField(Farm, on_delete= models.CASCADE)
    address = models.URLField(blank= False, null= False)
    username = models.CharField(max_length= 20, blank= False, null= False)
    password = models.CharField(max_length= 20, blank= False, null= False)
    token= models.TextField(default='', blank=True, null=True)
    point_check_expired = models.CharField(max_length= 200, default= 'GT1.Grid.totW')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.username

class PointType(models.Model):
    SCOPE_CHOICES = (
        ('farm', 'Farm Level'),
        ('turbine', 'Turbine Level'),
    )

    key = models.CharField(max_length= 50, unique= True) #ACTIVE_POWER, WIND_SPEED
    name = models.CharField(max_length= 50) #Power, Wind Speed
    level = models.CharField(max_length= 20, choices= SCOPE_CHOICES) #farm, turbine
    column_name = models.CharField(max_length=50)  # Tên cột trong DB (power, wind_speed)

    def __str__(self):
        return f"{self.name} ({self.key})"
    
class HISPoint(models.Model):
    farm = models.ForeignKey(Farm, on_delete= models.CASCADE, related_name= "point_HIS")
    point_type = models.ForeignKey(PointType, on_delete= models.CASCADE)
    turbine = models.ForeignKey(Turbines, on_delete= models.CASCADE, null= True, blank= True,
                                related_name= 'point_HIS')
    point_name = models.CharField(max_length= 200)
    is_active = models.BooleanField(default= True)
    created_at = models.DateTimeField(auto_now_add= True)
    updated_at = models.DateTimeField(auto_now= True)

    def clean(self):
        """Validation logic cho HISPoint"""
        from django.core.exceptions import ValidationError
        from django.utils.translation import gettext_lazy as _
        
        # Nếu point_type.level là 'turbine' thì phải có turbine
        if self.point_type and self.point_type.level == 'turbine' and not self.turbine:
            raise ValidationError(_('Turbine level point type requires a turbine'))
        
        # Nếu point_type.level là 'farm' thì không nên có turbine
        if self.point_type and self.point_type.level == 'farm' and self.turbine:
            raise ValidationError(_('Farm level point type should not have a turbine'))
    
    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    class Meta:
        unique_together = [
            ['farm', 'point_type', 'turbine']
        ]
    
    def __str__(self):
        if self.turbine:
            return f"{self.farm.name} - {self.turbine.name} - {self.point_type.name}"
        return f"{self.farm.name} - {self.point_type.name}"


class FactoryHistorical(models.Model):
    farm = models.ForeignKey(Farm, models.DO_NOTHING, related_name='acquisition_historical')
    time_stamp = models.DateTimeField(null=False)
    # Power data
    active_power = models.FloatField(null=True, verbose_name='Power generate of farm (MW)')
    # Weather data
    wind_speed = models.FloatField(null=True, verbose_name='Wind speed at 100m (m/s)')
    wind_dir = models.FloatField(null=True, verbose_name='Wind direction at 100m')
    air_temp = models.FloatField(null=True, verbose_name='Ambient Temperature (oC)')
    pressure = models.FloatField(null=True, verbose_name='Air pressure of farm (%)')
    hud = models.FloatField(null=True, verbose_name='Relative humidity of farm (%)')
    
    class Meta:
        ordering = ('time_stamp',)
        unique_together = ('farm', 'time_stamp')
        verbose_name = 'Factory Historical Data'
        verbose_name_plural = 'Factory Historical Data'
    
    def __str__(self):
        return f'{self.farm.name} - {self.time_stamp}'