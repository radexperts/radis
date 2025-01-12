from unittest.mock import patch

import pytest
from django.db import close_old_connections
from pytest_mock import MockerFixture

from radis.chats.utils.testing_helpers import create_async_openai_client_mock
from radis.extractions.processors import ExtractionTaskProcessor
from radis.extractions.utils.testing_helpers import create_extraction_task


@pytest.mark.django_db(transaction=True)
def test_extraction_task_processor(mocker: MockerFixture):
    num_filter_fields = 5
    num_data_fields = 5
    num_extraction_instances = 5
    task = create_extraction_task(
        language_code="en",
        num_filter_fields=num_filter_fields,
        num_data_fields=num_data_fields,
        num_extraction_instances=num_extraction_instances,
    )

    openai_mock = create_async_openai_client_mock("Yes")
    process_extraction_task_spy = mocker.spy(ExtractionTaskProcessor, "process_task")
    process_extraction_instance_spy = mocker.spy(
        ExtractionTaskProcessor, "process_extraction_instance"
    )

    with patch("openai.AsyncOpenAI", return_value=openai_mock):
        ExtractionTaskProcessor(task).start()

        for instance in task.instances.all():
            assert instance.is_processed
            assert instance.is_accepted

        assert process_extraction_task_spy.call_count == 1
        assert process_extraction_instance_spy.call_count == num_rag_instances
        assert process_yes_or_no_question_spy.call_count == num_rag_instances * num_questions
        assert openai_mock.chat.completions.create.call_count == num_rag_instances * num_questions

    close_old_connections()
