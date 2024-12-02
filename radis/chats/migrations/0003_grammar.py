# Generated by Django 5.1.2 on 2024-11-03 15:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("chats", "0002_chat_report"),
    ]

    operations = [
        migrations.CreateModel(
            name="Grammar",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=100)),
                ("human_readable_name", models.CharField(max_length=100)),
                ("grammar", models.TextField()),
                ("llm_instruction", models.TextField()),
                ("is_default", models.BooleanField(default=False)),
            ],
        ),
    ]
