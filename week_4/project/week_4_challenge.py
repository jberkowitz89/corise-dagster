# pyright: reportMissingImports=false
from random import randint

from dagster import AssetIn, asset, with_resources, AssetKey
from dagster_dbt import load_assets_from_dbt_project, dbt_cli_resource
from project.dbt_config import DBT_PROJECT_PATH
from project.resources import postgres_resource


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
)
def create_dbt_table(context):
    sql = "CREATE SCHEMA IF NOT EXISTS analytics;"
    context.resources.database.execute_query(sql)
    sql = "CREATE TABLE IF NOT EXISTS analytics.dbt_table (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)


@asset(
    required_resource_keys={"database"},
    op_tags={"kind": "postgres"},
    key_prefix="postgresql",
)
def dbt_table(context, create_dbt_table):
    sql = "INSERT INTO analytics.dbt_table (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


dbt_assets = with_resources(
    load_assets_from_dbt_project(project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROJECT_PATH),
    {
        "dbt": dbt_cli_resource.configured(
            {"project_dir": DBT_PROJECT_PATH,
             "profiles_dir": DBT_PROJECT_PATH,
             "ignore_handled_error": True,
             "target": "test"},
        )
    },
)


@asset(
    non_argument_deps={"my_second_dbt_model"}
)
def final(context):
    context.log.info("Week 4 Challenge completed")


create_dbt_table_docker, dbt_table_docker = with_resources(
    definitions=[create_dbt_table, dbt_table],
    resource_defs={"database": postgres_resource},
    resource_config_by_key={
        "database": {
            "config": {
                "host": "postgresql",
                "user": "postgres_user",
                "password": "postgres_password",
                "database": "postgres_db",
            }
        }
    }
)