# Generated by Django 5.0 on 2023-12-31 15:18

from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trackeditems', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='trackedorganization',
            options={'ordering': ['organization_uri']},
        ),
        migrations.RemoveConstraint(
            model_name='trackedorganization',
            name='trackeditems_unique_user_organization_name',
        ),
        migrations.RemoveField(
            model_name='trackedorganization',
            name='organization_name',
        ),
        migrations.AddField(
            model_name='trackedorganization',
            name='organization_uri',
            field=models.URLField(default='http://foo.example.org'),
            preserve_default=False,
        ),
        migrations.AddConstraint(
            model_name='trackedorganization',
            constraint=models.UniqueConstraint(models.F('user'), models.F('organization_uri'), name='trackeditems_unique_user_organization_uri'),
        ),
    ]
