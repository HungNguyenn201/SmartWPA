from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("analytics", "0005_computation_algorithm_identity_and_indicator_capacity_factor"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailyproduction",
            name="daily_reachable",
            field=models.FloatField(
                blank=True,
                help_text="Daily reachable (theoretical) production value (kWh). NULL for legacy data.",
                null=True,
            ),
        ),
    ]
