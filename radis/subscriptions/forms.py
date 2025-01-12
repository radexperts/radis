from typing import Any

from betterforms.multiform import MultiForm
from crispy_forms.helper import FormHelper
from crispy_forms.layout import HTML, Column, Div, Field, Fieldset, Layout, Row
from django import forms

from radis.core.constants import LANGUAGE_LABELS
from radis.extractions.site import retrieval_providers
from radis.reports.models import Language, Modality
from radis.search.forms import AGE_STEP, MAX_AGE, MIN_AGE
from radis.search.layouts import RangeSlider

from .models import FilterField, Subscription


class SubscriptionForm(forms.ModelForm):
    class Meta:
        model = Subscription
        fields = [
            "name",
            "provider",
            "query",
            "language",
            "modalities",
            "study_description",
            "patient_sex",
            "age_from",
            "age_till",
            "patient_id",
            "send_finished_mail",
        ]
        labels = {"patient_id": "Patient ID"}
        help_texts = {
            "name": "Name of the Subscription",
            "provider": "The search provider to use for the database query",
            "query": "A query to filter reports",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["provider"].widget = forms.Select(
            choices=sorted(
                [("", "")]
                + [(provider.name, provider.name) for provider in retrieval_providers.values()]
            )
        )
        self.fields["language"].choices = [  # type: ignore
            (language.pk, LANGUAGE_LABELS[language.code])
            for language in Language.objects.order_by("code")
        ]
        self.fields["language"].empty_label = "All"  # type: ignore
        self.fields["modalities"].choices = [  # type: ignore
            (modality.pk, modality.code)
            for modality in Modality.objects.filter(filterable=True).order_by("code")
        ]
        self.fields["age_from"] = forms.IntegerField(
            required=False,
            min_value=MIN_AGE,
            max_value=MAX_AGE,
            widget=forms.NumberInput(
                attrs={
                    "type": "range",
                    "step": AGE_STEP,
                    "value": MIN_AGE,
                }
            ),
        )
        self.fields["age_till"] = forms.IntegerField(
            required=False,
            min_value=MIN_AGE,
            max_value=MAX_AGE,
            widget=forms.NumberInput(
                attrs={
                    "type": "range",
                    "step": AGE_STEP,
                    "value": MAX_AGE,
                }
            ),
        )
        self.fields["study_description"] = forms.CharField(
            required=False,
            widget=forms.Textarea(attrs={"rows": 4}),
        )
        self.fields["send_finished_mail"].label = "Notify me via mail"

        self.helper = FormHelper()
        self.helper.form_tag = False
        self.helper.layout = self.build_layout()

    def build_layout(self):
        return Layout(
            Fieldset(
                "General",
                Row(
                    "name",
                ),
                Row(
                    "send_finished_mail",
                ),
            ),
            Fieldset(
                "Search",
                Row(
                    "provider",
                ),
                Row(
                    Column(
                        "query",
                        css_class="col-6",
                    ),
                    Column(
                        "language",
                        css_class="col-6",
                    ),
                ),
            ),
            Fieldset(
                "Filters",
                Row(
                    Column(
                        "patient_id",
                        css_class="col-4",
                    ),
                    Column(
                        "patient_sex",
                        css_class="col-4",
                    ),
                    Column(
                        RangeSlider("Patient age", "age_from", "age_till"),
                        css_class="col-4",
                    ),
                ),
                Row(
                    Column(
                        "modalities",
                        css_class="col-4",
                    ),
                    Column(
                        "study_description",
                        css_class="col-8",
                    ),
                ),
            ),
        )

    def clean_age_from(self) -> int:
        age_from = self.cleaned_data["age_from"]
        if age_from is not None and age_from % AGE_STEP != 0:
            raise forms.ValidationError(f"Age from must be a multiple of {AGE_STEP}")
        return age_from

    def clean_age_till(self) -> int:
        age_till = self.cleaned_data["age_till"]
        if age_till is not None and age_till % AGE_STEP != 0:
            raise forms.ValidationError(f"Age till must be a multiple of {AGE_STEP}")
        return age_till

    def clean(self) -> dict[str, Any] | None:
        age_from = self.cleaned_data["age_from"]
        age_till = self.cleaned_data["age_till"]

        if age_from is not None and age_till is not None and age_from >= age_till:
            raise forms.ValidationError("Age from must be less than age till")

        provider = self.cleaned_data["provider"]
        query = self.cleaned_data["query"]
        if query != "" and not provider:
            raise forms.ValidationError(
                "Setup of RADIS is incomplete. No retrieval providers are registered."
            )

        return super().clean()


class FilterFieldForm(forms.ModelForm):
    delete_button: str = """
        {% load bootstrap_icon from common_extras %}
        <button type="button"
                class="btn btn-sm btn-outline-danger d-none position-absolute top-0 end-0"
                :class="{'d-none': formCount === 0}"
                @click="removeForm($el)"
                aria-label="Remove Field">
            {% bootstrap_icon 'trash' %}
        </button>
    """

    class Meta:
        model = FilterField
        fields = [
            "name",
            "description",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.helper = FormHelper()
        self.helper.form_tag = False
        self.helper.disable_csrf = True
        self.helper.layout = Layout(
            Div(
                Div(
                    Field("id", type="hidden"),
                    "name",
                    "description",
                    Field("DELETE", type="hidden"),
                    css_class="card-body",
                ),
                HTML(self.delete_button),
                css_class="card mb-3",
            ),
        )


FilterFieldFormSet = forms.inlineformset_factory(
    Subscription,
    FilterField,
    form=FilterFieldForm,
    extra=0,
    min_num=0,
    max_num=3,
    validate_max=True,
    can_delete=True,
)


class SubscriptionAndFilterFieldsForm(MultiForm, forms.ModelForm):
    def get_form_args_kwargs(self, key, args, kwargs):
        args, fkwargs = super().get_form_args_kwargs(key, args, kwargs)

        if kwargs.get("instance", None) is not None:
            fkwargs["instance"] = kwargs["instance"].get(key)

        if kwargs.get("queryset", None) is not None:
            if key in kwargs["queryset"]:
                fkwargs["queryset"] = kwargs["queryset"][key]
            else:
                del fkwargs["queryset"]

        return args, fkwargs

    form_classes = {
        "subscription": SubscriptionForm,
        "filter_fields": FilterFieldFormSet,
    }
    form_order = ["subscription", "filter_fields"]
