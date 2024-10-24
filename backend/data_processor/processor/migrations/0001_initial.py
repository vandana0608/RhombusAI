# Generated by Django 4.2.16 on 2024-10-22 23:51

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="ProcessedFile",
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
                ("original_file", models.FileField(upload_to="uploads/")),
                (
                    "processed_file",
                    models.FileField(blank=True, null=True, upload_to="processed/"),
                ),
                ("upload_date", models.DateTimeField(auto_now_add=True)),
                ("processing_complete", models.BooleanField(default=False)),
            ],
        ),
    ]
