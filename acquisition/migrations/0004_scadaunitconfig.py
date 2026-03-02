from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("acquisition", "0003_remove_factoryhistorical_unique_farm_timestamp_and_more"),
        ("facilities", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="ScadaUnitConfig",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("data_source", models.CharField(choices=[("db", "Database"), ("file", "CSV Files"), ("any", "Any")], default="any", max_length=8)),
                ("active_power_unit", models.CharField(choices=[("kW", "kW"), ("MW", "MW"), ("W", "W")], default="kW", max_length=4)),
                ("wind_speed_unit", models.CharField(choices=[("m/s", "m/s"), ("km/h", "km/h")], default="m/s", max_length=4)),
                ("temperature_unit", models.CharField(choices=[("K", "Kelvin (K)"), ("C", "Celsius (°C)")], default="K", max_length=1)),
                (
                    "pressure_unit",
                    models.CharField(
                        choices=[
                            ("Pa", "Pa"),
                            ("hPa", "hPa"),
                            ("kPa", "kPa"),
                            ("mbar", "mbar"),
                            ("bar", "bar"),
                            ("percent", "Percent (not physical pressure)"),
                            ("unknown", "Unknown"),
                        ],
                        default="Pa",
                        max_length=10,
                    ),
                ),
                ("humidity_unit", models.CharField(choices=[("ratio", "Ratio (0..1)"), ("percent", "Percent (0..100)")], default="ratio", max_length=7)),
                ("active_power_multiplier", models.FloatField(default=1.0)),
                ("wind_speed_multiplier", models.FloatField(default=1.0)),
                ("temperature_multiplier", models.FloatField(default=1.0)),
                ("pressure_multiplier", models.FloatField(default=1.0)),
                ("humidity_multiplier", models.FloatField(default=1.0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "farm",
                    models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="scada_unit_configs", to="facilities.farm"),
                ),
                (
                    "turbine",
                    models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="scada_unit_configs", to="facilities.turbines"),
                ),
            ],
            options={
                "ordering": ("-updated_at", "-created_at"),
                "indexes": [
                    models.Index(fields=["farm", "turbine", "data_source"], name="acquisition_farm_turbine_source_idx"),
                    models.Index(fields=["turbine", "data_source"], name="acquisition_turbine_source_idx"),
                    models.Index(fields=["farm", "data_source"], name="acquisition_farm_source_idx"),
                ],
            },
        ),
    ]

