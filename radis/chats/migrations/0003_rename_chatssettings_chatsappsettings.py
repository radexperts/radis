# Generated by Django 5.1.4 on 2025-01-02 16:38

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("chats", "0002_chat_report"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="ChatsSettings",
            new_name="ChatsAppSettings",
        ),
    ]
