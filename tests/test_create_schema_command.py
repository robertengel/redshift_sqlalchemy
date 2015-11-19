from unittest import TestCase
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import CreateSchema
import re


class TestCreateSchemaCommand(TestCase):

    def setUp(self):
        pass

    def test_basic_create_schema(self):

        expected_result = re.sub(r'\s+', '', "CREATE SCHEMA abc").strip()
        engine = create_engine('redshift+psycopg2://')
        ddl_statement = re.sub(r'\s+', '', str(CreateSchema("abc").compile(engine)).strip())
        self.assertEqual(expected_result, ddl_statement)

    def test_if_not_exists_create_schema(self):

        expected_result = re.sub(r'\s+', ' ', "CREATE SCHEMA IF NOT EXISTS abc").strip()
        engine = create_engine('redshift+psycopg2://')
        ddl_statement = re.sub(r'\s+', ' ', str(CreateSchema("abc").compile(engine, compile_kwargs={"if_not_exists": True})).strip())
        self.assertEqual(expected_result, ddl_statement)
