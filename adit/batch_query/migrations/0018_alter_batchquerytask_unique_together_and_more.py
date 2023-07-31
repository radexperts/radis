# Generated by Django 4.2.3 on 2023-07-27 15:18

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("batch_query", "0017_alter_batchquerytask_lines"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="batchquerytask",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="batchquerytask",
            constraint=models.UniqueConstraint(
                fields=("job", "task_id"), name="batchquerytask_unique_task_id_per_job"
            ),
        ),
    ]
