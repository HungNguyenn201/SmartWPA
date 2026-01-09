from django.db import models
from facilities.models import Turbines, Farm
from django.core.validators import MinValueValidator



class Computation(models.Model):
    """Model to store computation metadata for various turbine analyses"""
    turbine = models.ForeignKey(Turbines, on_delete=models.CASCADE, related_name='computations')
    farm = models.ForeignKey(Farm, on_delete=models.CASCADE, related_name='turbine_computations')
    start_time = models.BigIntegerField(help_text="Start time in milliseconds")
    end_time = models.BigIntegerField(help_text="End time in milliseconds")
    computation_type = models.CharField(max_length=50, help_text="Type of computation (power_curve, classification, weibull, etc.)")
    created_at = models.DateTimeField(auto_now_add=True)
    is_latest = models.BooleanField(default=True, help_text="Whether this is the latest computation for this time range")
    v_cutin = models.FloatField(null=True, blank=True, help_text="Cut-in wind speed (m/s) - estimated from SCADA")
    v_cutout = models.FloatField(null=True, blank=True, help_text="Cut-out wind speed (m/s) - estimated from SCADA")
    v_rated = models.FloatField(null=True, blank=True, help_text="Rated wind speed (m/s) - estimated from SCADA")
    p_rated = models.FloatField(null=True, blank=True, help_text="Rated power (kW) - estimated from SCADA")
    class Meta:
        indexes = [
            models.Index(fields=['turbine', 'computation_type', '-start_time']),
            models.Index(fields=['turbine', 'computation_type', '-end_time']),
            models.Index(fields=['farm', 'computation_type', '-start_time']),
            models.Index(fields=['turbine', 'computation_type', 'is_latest']),
        ]
        ordering = ['-end_time', '-created_at']  # Most recent first
        # Remove unique constraint to allow multiple computations for same time range
        constraints = [
            models.UniqueConstraint(
                fields=['turbine', 'computation_type', 'start_time', 'end_time', 'is_latest'],
                name='unique_latest_computation'
            )
        ]

    def __str__(self):
        return f"{self.computation_type.title()} - {self.turbine.name} ({self.start_time} to {self.end_time})"

    def save(self, *args, **kwargs):
        # If this is marked as latest, unmark other computations for same time range
        if self.is_latest:
            Computation.objects.filter(
                turbine=self.turbine,
                computation_type=self.computation_type,
                start_time=self.start_time,
                end_time=self.end_time,
                is_latest=True
            ).exclude(pk=self.pk).update(is_latest=False)
        super().save(*args, **kwargs)


class PowerCurveAnalysis(models.Model):
    """Model to store power curve analysis results"""
    computation = models.ForeignKey(Computation, 
                                    on_delete=models.CASCADE, 
                                    related_name='power_curve_analyses')
    ANALYSIS_MODES = [
        ('global', 'Chế độ toàn cục'),
        ('yearly', 'Chế độ theo năm'),
        ('quarterly', 'Chế độ theo quý'),
        ('monthly', 'Chế độ theo tháng'),
        ('day/night', 'Chế độ ngày/đêm'),
    ]
    
    analysis_mode = models.CharField(max_length=20, choices=ANALYSIS_MODES, default='global')
    split_value = models.CharField(max_length=20, null=True, blank=True, 
                                 help_text="Value for the split (e.g. 'day'/'night' for day_night mode, '1' for January in monthly mode)")
    data_source = models.CharField(max_length=50, null=True, blank=True)
    min_value = models.FloatField(null=True, blank=True)
    max_value = models.FloatField(null=True, blank=True)
    

class PowerCurveData(models.Model):
    """Model to store individual data points in a turbine power curve"""
    analysis = models.ForeignKey(PowerCurveAnalysis, on_delete=models.CASCADE, related_name='power_curve_points', null=True, blank=True)
    wind_speed = models.FloatField()
    active_power = models.FloatField()
    
    class Meta:
        ordering = ['wind_speed']



class ClassificationSummary(models.Model):
    """Model to store classification summary statistics for each status type"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='classification_summary')
    status_code = models.IntegerField(help_text="Classification status code (0-7)")
    status_name = models.CharField(max_length=50, help_text="Human readable status name")
    count = models.IntegerField(help_text="Number of data points with this status")
    percentage = models.FloatField(help_text="Percentage of total data points")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation', 'status_code']),
        ]
        ordering = ['status_code']
        unique_together = ['computation', 'status_code']
        
    def __str__(self):
        return f"Status {self.status_code} ({self.status_name}): {self.count} points ({self.percentage:.2f}%)"


class ClassificationPoint(models.Model):
    """Model to store individual classification data points with timestamp and measurements"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='classification_points')
    timestamp = models.BigIntegerField(help_text="Timestamp in milliseconds")
    wind_speed = models.FloatField(help_text="Wind speed at this point")
    active_power = models.FloatField(help_text="Active power at this point")
    classification = models.IntegerField(help_text="Classification status code for this point")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation', 'timestamp']),
            models.Index(fields=['computation', 'classification']),
        ]
        ordering = ['timestamp']
        
    def __str__(self):
        return f"Point at {self.timestamp}: classification {self.classification}"


class IndicatorData(models.Model):
    """Model to store turbine performance indicators"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='indicator_data')
    
    # Basic indicators
    average_wind_speed = models.FloatField(validators=[MinValueValidator(0.0)])
    reachable_energy = models.FloatField(validators=[MinValueValidator(0.0)])
    real_energy = models.FloatField(validators=[MinValueValidator(0.0)])
    loss_energy = models.FloatField()
    loss_percent = models.FloatField()
    rated_power = models.FloatField(validators=[MinValueValidator(0.0)])
    tba = models.FloatField()  # Technical availability
    pba = models.FloatField()  # Performance availability
    
    # Loss indicators
    stop_loss = models.FloatField()
    partial_stop_loss = models.FloatField()
    under_production_loss = models.FloatField()
    curtailment_loss = models.FloatField()
    partial_curtailment_loss = models.FloatField()
    
    # Point counts
    total_stop_points = models.IntegerField()
    total_partial_stop_points = models.IntegerField()
    total_under_production_points = models.IntegerField()
    total_curtailment_points = models.IntegerField()
    
    # Time indicators
    mtbf = models.FloatField(null=True, blank=True, help_text="Mean Time Between Failures")  # Mean Time Between Failures
    mttr = models.FloatField(null=True, blank=True, help_text="Mean Time To Repair")  # Mean Time To Repair
    mttf = models.FloatField(null=True, blank=True, help_text="Mean Time To Failure")  # Mean Time To Failure
    time_step = models.FloatField()
    total_duration = models.FloatField()
    duration_without_error = models.FloatField()
    
    # Period indicators
    up_periods_count = models.FloatField()
    down_periods_count = models.FloatField()
    up_periods_duration = models.FloatField()
    down_periods_duration = models.FloatField()
    
    # Weibull AEP indicators
    aep_weibull_turbine = models.FloatField()
    aep_weibull_wind_farm = models.FloatField(null=True, blank=True, help_text="Wind farm AEP (not calculated in current computation)")
    
    # Rayleigh AEP indicators
    aep_rayleigh_measured_4 = models.FloatField()
    aep_rayleigh_measured_5 = models.FloatField()
    aep_rayleigh_measured_6 = models.FloatField()
    aep_rayleigh_measured_7 = models.FloatField()
    aep_rayleigh_measured_8 = models.FloatField()
    aep_rayleigh_measured_9 = models.FloatField()
    aep_rayleigh_measured_10 = models.FloatField()
    aep_rayleigh_measured_11 = models.FloatField()
    
    aep_rayleigh_extrapolated_4 = models.FloatField()
    aep_rayleigh_extrapolated_5 = models.FloatField()
    aep_rayleigh_extrapolated_6 = models.FloatField()
    aep_rayleigh_extrapolated_7 = models.FloatField()
    aep_rayleigh_extrapolated_8 = models.FloatField()
    aep_rayleigh_extrapolated_9 = models.FloatField()
    aep_rayleigh_extrapolated_10 = models.FloatField()
    aep_rayleigh_extrapolated_11 = models.FloatField()
    
    # Yaw indicators (optional)
    yaw_misalignment = models.FloatField(null=True, blank=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['computation']),
        ]

    def __str__(self):
        return f"Indicators for computation {self.computation.id}"


class WeibullData(models.Model):
    """Model to store weibull calculation results"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='weibull_data')
    scale_parameter_a = models.FloatField(help_text="Weibull scale parameter (A)")
    shape_parameter_k = models.FloatField(help_text="Weibull shape parameter (K)")
    mean_wind_speed = models.FloatField(null=True, blank=True, help_text="Mean wind speed (Vmean) - calculated from data if available")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation']),
        ]
        
    def __str__(self):
        return f"Weibull data for computation {self.computation.id} (A={self.scale_parameter_a}, K={self.shape_parameter_k})"


class YawErrorData(models.Model):
    """Model to store yaw error distribution points"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='yaw_error_points')
    angle = models.FloatField(help_text="Yaw error angle (X value)")
    frequency = models.FloatField(help_text="Frequency/count at this angle (Y value)")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation']),
        ]
        ordering = ['angle']
        unique_together = ['computation', 'angle']
        
    def __str__(self):
        return f"Yaw error {self.angle}° (count: {self.frequency})"


class YawErrorStatistics(models.Model):
    """Model to store yaw error statistics for a computation"""
    computation = models.OneToOneField(Computation, on_delete=models.CASCADE, related_name='yaw_error_statistics')
    mean_error = models.FloatField(help_text="Mean yaw error")
    median_error = models.FloatField(help_text="Median yaw error")
    std_error = models.FloatField(help_text="Standard deviation of yaw error")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation']),
        ]
        
    def __str__(self):
        return f"Yaw statistics for computation {self.computation.id} (mean: {self.mean_error:.2f}°)"


class DailyProduction(models.Model):
    """Model to store daily production records for a computation"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='daily_productions')
    date = models.DateField(help_text="Production date (YYYY-MM-DD)")
    daily_production = models.FloatField(help_text="Daily production value")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation', 'date']),
            models.Index(fields=['computation', '-date']),
        ]
        ordering = ['date']
        unique_together = ['computation', 'date']
        
    def __str__(self):
        return f"Daily production {self.date}: {self.daily_production:.2f}"


class CapacityFactorData(models.Model):
    """Model to store capacity factor by wind speed bin for a computation"""
    computation = models.ForeignKey(Computation, on_delete=models.CASCADE, related_name='capacity_factors')
    wind_speed_bin = models.FloatField(help_text="Wind speed bin value")
    capacity_factor = models.FloatField(help_text="Capacity factor for this wind speed bin")
    
    class Meta:
        indexes = [
            models.Index(fields=['computation', 'wind_speed_bin']),
        ]
        ordering = ['wind_speed_bin']
        unique_together = ['computation', 'wind_speed_bin']
        
    def __str__(self):
        return f"Capacity factor at {self.wind_speed_bin} m/s: {self.capacity_factor:.4f}"

