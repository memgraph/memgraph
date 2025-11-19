# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys
from pathlib import Path

import boto3
import mgclient
import pytest
from common import connect, execute_and_fetch_all

# LocalStack configuration
# NOTE: If you are testing this locally change localstack-s3 to localhost
AWS_ENDPOINT_URL = "http://localstack-s3:4566"
# AWS_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"
AWS_REGION = "us-east-1"
BUCKET_NAME = "deps.memgraph.io"


def test_no_such_file():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = (
        f"LOAD PARQUET FROM 's3://{BUCKET_NAME}/nodes_no_file.parquet' WITH CONFIG {{'aws_region': '{AWS_REGION}', "
        f"'aws_access_key': '{AWS_ACCESS_KEY_ID}', 'aws_secret_key': '{AWS_SECRET_ACCESS_KEY}', 'aws_endpoint_url': '{AWS_ENDPOINT_URL}' }} AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    )
    try:
        execute_and_fetch_all(cursor, load_query)
        assert False
    except mgclient.DatabaseError:
        # No such file
        pass


def test_http_file():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = (
        f"LOAD PARQUET FROM '{AWS_ENDPOINT_URL}/{BUCKET_NAME}/nodes_100.parquet' WITH CONFIG {{'aws_region': '{AWS_REGION}', "
        f"'aws_access_key': '{AWS_ACCESS_KEY_ID}', 'aws_secret_key': '{AWS_SECRET_ACCESS_KEY}', 'aws_endpoint_url': '{AWS_ENDPOINT_URL}' }} AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_small_file_nodes_query_settings():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = (
        f"LOAD PARQUET FROM 's3://{BUCKET_NAME}/nodes_100.parquet' WITH CONFIG {{'aws_region': '{AWS_REGION}', "
        f"'aws_access_key': '{AWS_ACCESS_KEY_ID}', 'aws_secret_key': '{AWS_SECRET_ACCESS_KEY}', 'aws_endpoint_url': '{AWS_ENDPOINT_URL}' }} AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_small_file_nodes_runtime_setting():
    cursor = connect(host="localhost", port=7687).cursor()
    aws_region_setting = "aws.region"
    aws_access_setting = "aws.access_key"
    aws_secret_setting = "aws.secret_key"
    aws_endpoint_url_setting = "aws.endpoint_url"

    execute_and_fetch_all(cursor, f"set database setting '{aws_region_setting}' to '{AWS_REGION}'")
    execute_and_fetch_all(cursor, f"set database setting '{aws_access_setting}' to '{AWS_ACCESS_KEY_ID}'")
    execute_and_fetch_all(cursor, f"set database setting '{aws_secret_setting}' to '{AWS_SECRET_ACCESS_KEY}'")
    execute_and_fetch_all(cursor, f"set database setting '{aws_endpoint_url_setting}' to '{AWS_ENDPOINT_URL}'")

    settings = dict(execute_and_fetch_all(cursor, "show database settings"))
    assert settings[aws_region_setting] == AWS_REGION
    assert settings[aws_access_setting] == AWS_ACCESS_KEY_ID
    assert settings[aws_secret_setting] == AWS_SECRET_ACCESS_KEY
    assert settings[aws_endpoint_url_setting] == AWS_ENDPOINT_URL

    load_query = f"LOAD PARQUET FROM 's3://{BUCKET_NAME}/nodes_100.parquet' AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def main():
    # Configure S3 client for LocalStack
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' already exists")
    except:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created bucket '{BUCKET_NAME}'")

    # Upload parquet file
    parquet_file = Path(__file__).parent / "nodes_100.parquet"

    if not parquet_file.exists():
        print(f"Error: File {parquet_file} does not exist")
        sys.exit(1)

    print(f"Uploading {parquet_file} to s3://{BUCKET_NAME}/nodes_100.parquet")
    s3_client.upload_file(str(parquet_file), BUCKET_NAME, "nodes_100.parquet")
    print("Upload successful!")

    # List files in bucket to verify
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" in response:
        print(f"\nFiles in bucket '{BUCKET_NAME}':")
        for obj in response["Contents"]:
            print(f"  - s3://{BUCKET_NAME}/{obj['Key']} (Size: {obj['Size']} bytes)")


if __name__ == "__main__":
    main()
    sys.exit(pytest.main([__file__, "-rA"]))
