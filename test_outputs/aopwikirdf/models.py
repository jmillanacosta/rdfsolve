"""Pydantic models generated from aopwikirdf schema."""

from pydantic import BaseModel, Field
from typing import Optional, List


class Person(BaseModel):
    """A person in the system"""
    name: <class 'str'>
    email: list[str]
    age: int | None
    knows: list[str]
    worksFor: str | None


class Organization(BaseModel):
    """A company or organization"""
    orgName: <class 'str'>
    employees: list[rdfsolve.schema_models.jsonschema.Person]


