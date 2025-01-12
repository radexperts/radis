from typing import Any

from django.db.models import QuerySet
from pydantic import BaseModel, create_model

from radis.extractions.models import BaseDataField, BaseFilterField, DataType


def generate_filter_fields_schema(fields: QuerySet[BaseFilterField]) -> type[BaseModel]:
    field_definitions: dict[str, Any] = {}
    for field in fields.all():
        field_definitions[field.name] = (bool, ...)

    return create_model("FilterFieldsModel", field_definitions=field_definitions)


def generate_filter_fields_prompt(fields: QuerySet[BaseFilterField]) -> str:
    prompt = ""
    for field in fields.all():
        prompt += f"{field.name}: {field.description}\n"

    return prompt


type Numeric = float | int


def generate_data_fields_schema(fields: QuerySet[BaseDataField]) -> type[BaseModel]:
    field_definitions: dict[str, Any] = {}
    for field in fields.all():
        if field.data_type == DataType.TEXT:
            data_type = str
        elif field.data_type == DataType.NUMERIC:
            data_type = Numeric
        elif field.data_type == DataType.BOOLEAN:
            data_type = bool
        else:
            raise ValueError(f"Unknown data type: {field.data_type}")

        field_definitions[field.name] = (data_type, ...)

    return create_model("DataFieldsModel", field_definitions=field_definitions)


def generate_data_fields_prompt(fields: QuerySet[BaseDataField]) -> str:
    prompt = ""
    for field in fields.all():
        prompt += f"{field.name}: {field.description}\n"

    return prompt
