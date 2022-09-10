# pyright: reportMissingImports=false
from typing import List
from operator import attrgetter

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
    description="Getting a list of stocks from a file in S3"
)
def get_s3_data(context):
    key_name = context.op_config["s3_key"]
    output = list()
    for record in context.resources.s3.get_data(key_name):
        stock = Stock.from_list(record)
        output.append(stock)
    return output


@asset(
    group_name="corise",
    description="Find the highest high value from a list of stocks",
    op_tags={"kind":"python"}
)
def process_data(get_s3_data) -> Aggregation:
    high_stock = max(get_s3_data, key=attrgetter('high'))
    agg = Aggregation(date=high_stock.date, high=high_stock.high)
    return agg


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    op_tags={"kind":"redis"},
    description="Put Aggregation data into Redis",
)
def put_redis_data(context, process_data):
    date = str(process_data.date)
    high = str(process_data.high)
    context.log.info(f"Putting stock {date} with high of {high} into Redis")
    context.resources.redis.put_data(date, high)


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource,
                   "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
              }
            },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
                }
            }
    }
)
