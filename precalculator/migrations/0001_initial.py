# Generated by Django 5.0.1 on 2024-06-29 23:56

import precalculator.models
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='PrecalculatedData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key', models.TextField(db_index=True, unique=True)),
                ('json_value', models.JSONField(encoder=precalculator.models.CustomEncoder, null=True)),
                ('datetime_value', models.DateTimeField(null=True)),
            ],
        ),
    ]
