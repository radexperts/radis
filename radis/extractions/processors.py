import logging
from concurrent.futures import ThreadPoolExecutor
from string import Template

from django import db
from django.conf import settings

from radis.chats.utils.chat_client import ChatClient
from radis.core.processors import AnalysisTaskProcessor
from radis.extractions.utils.processor_utils import (
    generate_data_fields_prompt,
    generate_data_fields_schema,
    generate_filter_fields_prompt,
    generate_filter_fields_schema,
)

from .models import ExtractionInstance, ExtractionTask

logger = logging.getLogger(__name__)


class ExtractionTaskProcessor(AnalysisTaskProcessor):
    def __init__(self, task: ExtractionTask) -> None:
        super().__init__(task)
        self.client = ChatClient()

    def process_task(self, task: ExtractionTask) -> None:
        with ThreadPoolExecutor(max_workers=settings.EXTRACTION_LLM_CONCURRENCY_LIMIT) as executor:
            try:
                for instance in task.instances.all():
                    executor.submit(self.process_instance, instance)
            finally:
                db.close_old_connections()

    def process_instance(self, instance: ExtractionInstance) -> None:
        assert not instance.is_processed

        instance.is_accepted = self.process_filter_fields(instance)
        instance.save()

        if instance.is_accepted:
            self.process_data_fields(instance)

        instance.is_processed = True
        instance.save()

        db.close_old_connections()

    def process_filter_fields(self, instance: ExtractionInstance) -> bool:
        job = instance.task.job
        Schema = generate_filter_fields_schema(job.filter_fields)
        prompt = Template(settings.FILTER_FIELDS_SYSTEM_PROMPT).substitute(
            {
                "report": instance.report.body,
                "fields": generate_filter_fields_prompt(job.filter_fields),
            }
        )
        result = self.client.extract_data(prompt, Schema)
        instance.filter_fields_result = result.model_dump()
        instance.save()

        for field_name in result.__pydantic_fields__:
            field_value = getattr(result, field_name)
            if not field_value:
                return False

        return True

    def process_data_fields(self, instance: ExtractionInstance) -> None:
        job = instance.task.job
        Schema = generate_data_fields_schema(job.data_fields)
        message = Template(settings.DATA_FIELDS_SYSTEM_PROMPT).substitute(
            {
                "report": instance.report.body,
                "fields": generate_data_fields_prompt(job.data_fields),
            }
        )
        result = self.client.extract_data(message, Schema)
        instance.data_fields_result = result.model_dump()
        instance.save()
