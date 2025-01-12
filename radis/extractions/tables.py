import django_tables2 as tables

from radis.core.tables import AnalysisJobTable, AnalysisTaskTable

from .models import ExtractionInstance, ExtractionJob, ExtractionTask


class ExtractionJobTable(AnalysisJobTable):
    class Meta(AnalysisJobTable.Meta):
        model = ExtractionJob


class ExtractionTaskTable(AnalysisTaskTable):
    class Meta(AnalysisTaskTable.Meta):
        model = ExtractionTask
        empty_text = "No extraction tasks to show"
        fields = ("id", "status", "message", "ended_at")


class ExtractionInstanceTable(tables.Table):
    class Meta:
        model = ExtractionInstance
        empty_text = "No extraction instances to show"
        fields = ("id", "is_processed", "is_accepted")
        attrs = {"class": "table table-bordered table-hover"}
