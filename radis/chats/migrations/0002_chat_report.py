# Generated by Django 5.1 on 2024-09-10 14:20

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('chats', '0001_initial'),
        ('reports', '0012_report_accession_number_and_study_instance_uid'),
    ]

    operations = [
        migrations.AddField(
            model_name='chat',
            name='report',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='chats', to='reports.report'),
        ),
    ]
