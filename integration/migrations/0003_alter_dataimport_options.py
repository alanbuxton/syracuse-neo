# Generated by Django 4.2.7 on 2023-12-27 02:10

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('integration', '0002_alter_dataimport_options_and_more'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='dataimport',
            options={'ordering': ['-run_at', '-import_ts']},
        ),
    ]
