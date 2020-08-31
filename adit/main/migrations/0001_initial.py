# Generated by Django 3.1 on 2020-08-31 00:54

import adit.main.fields
from django.conf import settings
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='AppSettings',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('maintenance_mode', models.BooleanField(default=False)),
            ],
            options={
                'verbose_name_plural': 'App settings',
            },
        ),
        migrations.CreateModel(
            name='DicomNode',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('node_name', models.CharField(max_length=64, unique=True)),
                ('node_type', models.CharField(choices=[('SV', 'Server'), ('FO', 'Folder')], max_length=2)),
            ],
        ),
        migrations.CreateModel(
            name='TransferJob',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('job_type', models.CharField(choices=[('ST', 'Selective Transfer'), ('BT', 'Batch Transfer')], max_length=2)),
                ('status', models.CharField(choices=[('UV', 'Unverified'), ('PE', 'Pending'), ('IP', 'In Progress'), ('CI', 'Canceling'), ('CA', 'Canceled'), ('SU', 'Success'), ('WA', 'Warning'), ('FA', 'Failure')], default='UV', max_length=2)),
                ('message', models.TextField(blank=True, null=True)),
                ('trial_protocol_id', models.CharField(blank=True, max_length=64, null=True)),
                ('trial_protocol_name', models.CharField(blank=True, max_length=64, null=True)),
                ('archive_password', models.CharField(blank=True, max_length=50, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('started_at', models.DateTimeField(null=True)),
                ('stopped_at', models.DateTimeField(null=True)),
                ('created_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='transfer_jobs', to=settings.AUTH_USER_MODEL)),
                ('destination', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to='main.dicomnode')),
                ('source', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to='main.dicomnode')),
            ],
        ),
        migrations.CreateModel(
            name='DicomFolder',
            fields=[
                ('dicomnode_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='main.dicomnode')),
                ('path', models.CharField(max_length=256)),
            ],
            bases=('main.dicomnode',),
        ),
        migrations.CreateModel(
            name='DicomServer',
            fields=[
                ('dicomnode_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='main.dicomnode')),
                ('ae_title', models.CharField(max_length=16, unique=True)),
                ('host', models.CharField(max_length=255)),
                ('port', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1), django.core.validators.MaxValueValidator(65535)])),
                ('patient_root_query_model_find', models.BooleanField(default=True)),
                ('patient_root_query_model_get', models.BooleanField(default=True)),
                ('patient_root_query_model_move', models.BooleanField(default=True)),
            ],
            bases=('main.dicomnode',),
        ),
        migrations.CreateModel(
            name='TransferTask',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('object_id', models.PositiveIntegerField(blank=True, null=True)),
                ('patient_id', models.CharField(max_length=64)),
                ('study_uid', models.CharField(max_length=64)),
                ('series_uids', adit.main.fields.SeparatedValuesField(blank=True, null=True)),
                ('pseudonym', models.CharField(blank=True, max_length=324, null=True)),
                ('status', models.CharField(choices=[('PE', 'Pending'), ('IP', 'In Progress'), ('CA', 'Canceled'), ('SU', 'Success'), ('FA', 'Failure')], default='PE', max_length=2)),
                ('message', models.TextField(blank=True, null=True)),
                ('log', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('started_at', models.DateTimeField(null=True)),
                ('stopped_at', models.DateTimeField(null=True)),
                ('content_type', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='contenttypes.contenttype')),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tasks', to='main.transferjob')),
            ],
        ),
        migrations.AddIndex(
            model_name='transferjob',
            index=models.Index(fields=['created_by', 'status'], name='main_transf_created_623d0e_idx'),
        ),
    ]
