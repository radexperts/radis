import logging
from concurrent.futures import ThreadPoolExecutor
from string import Template

from adit_radis_shared.common.types import User
from django import db
from django.conf import settings

from radis.chats.utils.chat_client import ChatClient
from radis.core.processors import AnalysisTaskProcessor
from radis.extractions.utils.processor_utils import (
    generate_filter_fields_prompt,
    generate_filter_fields_schema,
)
from radis.reports.models import Report

from .models import (
    SubscribedItem,
    Subscription,
    SubscriptionTask,
)

logger = logging.getLogger(__name__)


class SubscriptionTaskProcessor(AnalysisTaskProcessor):
    def __init__(self, task: SubscriptionTask) -> None:
        super().__init__(task)
        self.client = ChatClient()

    def process_task(self, task: SubscriptionTask) -> None:
        user: User = task.job.owner
        active_group = user.active_group

        with ThreadPoolExecutor(max_workers=settings.EXTRACTION_LLM_CONCURRENCY_LIMIT) as executor:
            try:
                for report in task.reports.filter(groups=active_group):
                    executor.submit(self.process_report, report, task)
            finally:
                db.close_old_connections()

    def process_report(self, report: Report, task: SubscriptionTask) -> None:
        subscription: Subscription = task.job.subscription
        Schema = generate_filter_fields_schema(subscription.filter_fields)
        prompt = Template(settings.FILTER_FIELDS_SYSTEM_PROMPT).substitute(
            {
                "report": report.body,
                "fields": generate_filter_fields_prompt(subscription.filter_fields),
            }
        )
        result = self.client.extract_data(prompt, Schema)

        is_accepted = all(
            [getattr(result, field_name) for field_name in result.__pydantic_fields__]
        )
        if is_accepted:
            SubscribedItem.objects.create(
                subscription=task.job.subscription,
                job=task.job,
                report=report,
                filter_fields_results=result.model_dump(),
            )
            logger.debug(f"Report {report.pk} was accepted by subscription {subscription.pk}")
        else:
            logger.debug(f"Report {report.pk} was rejected by subscription {subscription.pk}")
