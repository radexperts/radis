from typing import Literal

from adit_radis_shared.accounts.factories import GroupFactory, UserFactory
from adit_radis_shared.common.utils.testing_helpers import add_user_to_group

from radis.extractions.factories import (
    DataFieldFactory,
    ExtractionInstanceFactory,
    ExtractionJobFactory,
    ExtractionTaskFactory,
    FilterFieldFactory,
)
from radis.extractions.models import ExtractionJob, ExtractionTask
from radis.reports.factories import LanguageFactory, ReportFactory


def create_extraction_task(
    language_code: Literal["en", "de"] = "en",
    num_filter_fields: int = 5,
    num_data_fields: int = 5,
    num_extraction_instances: int = 5,
) -> ExtractionTask:
    language = LanguageFactory.create(code=language_code)

    user = UserFactory()
    group = GroupFactory()
    add_user_to_group(user, group)
    job = ExtractionJobFactory.create(
        status=ExtractionJob.Status.PENDING,
        owner_id=user.id,
        language=language,
    )

    FilterFieldFactory.create_batch(num_filter_fields, job=job)
    DataFieldFactory.create_batch(num_data_fields, job=job)

    task = ExtractionTaskFactory.create(job=job)

    for _ in range(num_extraction_instances):
        report = ReportFactory.create(language=language)
        ExtractionInstanceFactory.create(task=task, report=report)

    return task
