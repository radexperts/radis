from typing import Any, Type, cast

from adit_radis_shared.common.mixins import (
    PageSizeSelectMixin,
    RelatedPaginationMixin,
)
from adit_radis_shared.common.types import AuthenticatedHttpRequest
from django.conf import settings
from django.contrib.auth.mixins import (
    LoginRequiredMixin,
    PermissionRequiredMixin,
    UserPassesTestMixin,
)
from django.core.exceptions import SuspiciousOperation
from django.db import transaction
from django.db.models import QuerySet
from django.forms import BaseInlineFormSet
from django.shortcuts import redirect
from django.urls import reverse_lazy
from django.views.generic import DetailView
from django_tables2 import SingleTableMixin
from formtools.wizard.views import SessionWizardView

from radis.core.views import (
    AnalysisJobCancelView,
    AnalysisJobDeleteView,
    AnalysisJobDetailView,
    AnalysisJobListView,
    AnalysisJobRestartView,
    AnalysisJobResumeView,
    AnalysisJobRetryView,
    AnalysisJobVerifyView,
    AnalysisTaskDeleteView,
    AnalysisTaskDetailView,
    AnalysisTaskResetView,
    BaseUpdatePreferencesView,
)
from radis.reports.models import Language, Modality
from radis.search.site import Search, SearchFilters
from radis.search.utils.query_parser import QueryParser

from .filters import ExtractionInstanceFilter, ExtractionJobFilter, ExtractionTaskFilter
from .forms import (
    DataFieldFormSet,
    DataFieldFormSetHelper,
    FilterFieldFormSet,
    FilterFieldFormSetHelper,
    SearchForm,
    SummaryForm,
)
from .mixins import ExtractionsLockedMixin
from .models import ExtractionInstance, ExtractionJob, ExtractionTask
from .site import retrieval_providers
from .tables import ExtractionInstanceTable, ExtractionJobTable, ExtractionTaskTable

EXTRACTIONS_SEARCH_PROVIDER = "extractions_search_provider"


class ExtractionUpdatePreferencesView(ExtractionsLockedMixin, BaseUpdatePreferencesView):
    allowed_keys = [
        EXTRACTIONS_SEARCH_PROVIDER,
    ]


class ExtractionJobListView(ExtractionsLockedMixin, AnalysisJobListView):
    model = ExtractionJob
    table_class = ExtractionJobTable
    filterset_class = ExtractionJobFilter
    template_name = "extractions/extraction_job_list.html"


class ExtractionJobWizardView(
    LoginRequiredMixin, PermissionRequiredMixin, UserPassesTestMixin, SessionWizardView
):
    SEARCH_STEP = "0"
    FILTER_FIELDS_STEP = "1"
    DATA_FIELDS_STEP = "2"
    SUMMARY_STEP = "3"

    form_list = [SearchForm, FilterFieldFormSet, DataFieldFormSet, SummaryForm]
    permission_required = "extractions.add_extractionjob"
    permission_denied_message = "You must be logged in and have an active group"
    request: AuthenticatedHttpRequest

    def test_func(self) -> bool | None:
        return self.request.user.active_group is not None

    def get_form_kwargs(self, step=None):
        kwargs = super().get_form_kwargs(step)
        if step == ExtractionJobWizardView.SEARCH_STEP:
            kwargs["user"] = self.request.user
        return kwargs

    def get_context_data(self, form, **kwargs):
        context = super().get_context_data(form, **kwargs)

        if self.steps.current == ExtractionJobWizardView.SEARCH_STEP:
            pass

        elif self.steps.current == ExtractionJobWizardView.FILTER_FIELDS_STEP:
            context["helper"] = FilterFieldFormSetHelper()

            data = self.get_cleaned_data_for_step(ExtractionJobWizardView.SEARCH_STEP)
            assert data and isinstance(data, dict)
            context["provider"] = retrieval_providers[data["provider"]]

            active_group = self.request.user.active_group
            assert active_group

            language = cast(Language, data["language"])
            modalities = cast(QuerySet[Modality], data["modalities"])

            query_node, fixes = QueryParser().parse(data["query"])

            # It was already validated by the form in the first step that the query is not empty
            assert query_node

            if len(fixes) > 0:
                context["fixed_query"] = QueryParser.unparse(query_node)

            search = Search(
                query=query_node,
                offset=0,
                limit=0,
                filters=SearchFilters(
                    group=active_group.pk,
                    language=language.code,
                    modalities=list(modalities.values_list("code", flat=True)),
                    study_date_from=data["study_date_from"],
                    study_date_till=data["study_date_till"],
                    study_description=data["study_description"],
                    patient_sex=data["patient_sex"],
                    patient_age_from=data["age_from"],
                    patient_age_till=data["age_till"],
                ),
            )

            retrieval_provider = retrieval_providers[data["provider"]]
            context["retrieval_count"] = retrieval_provider.count(search)

        elif self.steps.current == ExtractionJobWizardView.DATA_FIELDS_STEP:
            context["helper"] = DataFieldFormSetHelper()

        elif self.steps.current == ExtractionJobWizardView.SUMMARY_STEP:
            search_data = self.get_cleaned_data_for_step(ExtractionJobWizardView.SEARCH_STEP)
            filter_fields = self.get_cleaned_data_for_step(
                ExtractionJobWizardView.FILTER_FIELDS_STEP
            )
            data_fields = self.get_cleaned_data_for_step(ExtractionJobWizardView.DATA_FIELDS_STEP)

            context["search"] = search_data
            context["filter_fields"] = filter_fields
            context["data_fields"] = data_fields

        return context

    def get_template_names(self) -> list[str]:
        step = self.steps.current
        if step == ExtractionJobWizardView.SEARCH_STEP:
            return ["extractions/extraction_job_search_form.html"]
        elif step == ExtractionJobWizardView.FILTER_FIELDS_STEP:
            return ["extractions/extraction_job_filter_fields_form.html"]
        elif step == ExtractionJobWizardView.DATA_FIELDS_STEP:
            return ["extractions/extraction_job_data_fields_form.html"]
        elif step == ExtractionJobWizardView.SUMMARY_STEP:
            return ["extractions/extraction_job_wizard_summary.html"]
        else:
            raise SuspiciousOperation(f"Invalid wizard step: {step}")

    def done(
        self,
        form_objs: tuple[SearchForm, BaseInlineFormSet, BaseInlineFormSet, SummaryForm],
        **kwargs,
    ):
        user = self.request.user

        with transaction.atomic():
            summary_form = form_objs[3]
            job_form = form_objs[0]
            if summary_form.cleaned_data["send_finished_mail"]:
                job_form.cleaned_data["send_finished_mail"] = True

            job: ExtractionJob = job_form.save(commit=False)

            query = job.query
            query_node, fixes = QueryParser().parse(query)
            if len(fixes) > 0:
                # The query was already validated that it is not empty by the form
                assert query_node
                job.query = QueryParser.unparse(query_node)

            group = user.active_group
            assert group

            job.group = group
            job.owner = user
            job.save()

            # Save filter fields
            form_objs[1].instance = job
            form_objs[1].save()

            # Save data fields
            form_objs[2].instance = job
            form_objs[2].save()

            if user.is_staff or settings.START_EXTRACTION_JOB_UNVERIFIED:
                job.status = ExtractionJob.Status.PREPARING
                job.save()

                transaction.on_commit(lambda: job.delay())

        return redirect(job)


class ExtractionJobDetailView(AnalysisJobDetailView):
    model = ExtractionJob
    table_class = ExtractionTaskTable
    filterset_class = ExtractionTaskFilter
    context_object_name = "job"
    template_name = "extractions/extraction_job_detail.html"


class ExtractionJobDeleteView(ExtractionsLockedMixin, AnalysisJobDeleteView):
    model = ExtractionJob
    success_url = reverse_lazy("extraction_job_list")


class ExtractionJobVerifyView(ExtractionsLockedMixin, AnalysisJobVerifyView):
    model = ExtractionJob


class ExtractionJobCancelView(ExtractionsLockedMixin, AnalysisJobCancelView):
    model = ExtractionJob


class ExtractionJobResumeView(ExtractionsLockedMixin, AnalysisJobResumeView):
    model = ExtractionJob


class ExtractionJobRetryView(ExtractionsLockedMixin, AnalysisJobRetryView):
    model = ExtractionJob


class ExtractionJobRestartView(ExtractionsLockedMixin, AnalysisJobRestartView):
    model = ExtractionJob


class ExtractionTaskDetailView(ExtractionsLockedMixin, AnalysisTaskDetailView, SingleTableMixin):
    model = ExtractionTask
    table_class = ExtractionInstanceTable
    filterset_class = ExtractionInstanceFilter
    job_url_name = "extraction_job_detail"
    template_name = "extractions/extraction_task_detail.html"

    def get_context_data(self, **kwargs) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        task = cast(ExtractionTask, self.get_object())
        self.object_list = task.instances.all()
        table = self.get_table()
        context[self.get_context_table_name(table)] = table
        return context


class ExtractionTaskDeleteView(ExtractionsLockedMixin, AnalysisTaskDeleteView):
    model = ExtractionTask


class ExtractionTaskResetView(ExtractionsLockedMixin, AnalysisTaskResetView):
    model = ExtractionTask


class ExtractionInstanceDetailView(LoginRequiredMixin, DetailView):
    context_object_name = "extraction_instance"
    template_name = "extractions/extraction_instance_detail.html"
    request: AuthenticatedHttpRequest

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context["task_url_name"] = "extraction_task_detail"
        context["job_url_name"] = "extraction_job_detail"
        return context

    def get_queryset(self) -> QuerySet[ExtractionInstance]:
        if self.request.user.is_staff:
            return ExtractionInstance.objects.all()
        return ExtractionInstance.objects.filter(task__job__owner=self.request.user)


class ExtractionResultListView(
    ExtractionsLockedMixin,
    LoginRequiredMixin,
    RelatedPaginationMixin,
    PageSizeSelectMixin,
    DetailView,
):
    model = ExtractionJob
    context_object_name = "job"
    template_name = "extractions/extraction_result_list.html"
    request: AuthenticatedHttpRequest
    object_list: QuerySet[ExtractionInstance]

    def get_queryset(self) -> QuerySet[ExtractionJob]:
        assert self.model
        model = cast(Type[ExtractionJob], self.model)
        if self.request.user.is_staff:
            return model.objects.all()
        return model.objects.filter(owner=self.request.user)

    def get_related_queryset(self) -> QuerySet[ExtractionInstance]:
        job = cast(ExtractionJob, self.get_object())
        return ExtractionInstance.objects.filter(task__job=job, is_accepted=True)

    def get_filter_queryset(self) -> QuerySet[ExtractionInstance]:
        return self.get_related_queryset()
