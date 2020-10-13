from django.db import models
from django.urls import reverse
import celery
from adit.main.models import AppSettings, TransferJob


class ContinuousTransferSettings(AppSettings):
    class Meta:
        verbose_name_plural = "Continuous transfer settings"


class ContinuousTransferJob(TransferJob):
    JOB_TYPE = "CT"

    project_name = models.CharField(max_length=150)
    project_description = models.TextField(max_length=2000)
    start_date = models.DateField()
    end_date = models.DateField(null=True, blank=True)

    def delay(self):
        celery.current_app.send_task(
            "adit.continuous_transfer.tasks.continuous_transfer", (self.id,)
        )

    def get_absolute_url(self):
        return reverse("continuous_transfer_job_detail", args=[str(self.id)])


class DataElementFilter(models.Model):
    class FilterTypes(models.TextChoices):
        EQUALS = "EQ", "equals"
        CONTAINS = "CO", "contains"
        CONTAINS_NOT = "CN", "contains not"
        REGEXP = "RE", "regexp"

    job = models.ForeignKey(
        ContinuousTransferJob, on_delete=models.CASCADE, related_name="filters"
    )
    dicom_tag = models.CharField(max_length=100)
    filter_type = models.CharField(
        max_length=2, choices=FilterTypes.choices, default=FilterTypes.CONTAINS
    )
    filter_value = models.CharField(max_length=200)
    case_sensitive = models.BooleanField(default=False)
