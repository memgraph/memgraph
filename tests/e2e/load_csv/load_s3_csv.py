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
import pytest
from common import connect, execute_and_fetch_all
from mgclient import DatabaseError

# LocalStack configuration
# NOTE: If you are testing this locally change localstack-s3 to localhost
AWS_ENDPOINT_URL = "http://localstack-s3:4566"
# AWS_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"
AWS_REGION = "us-east-1"
BUCKET_NAME = "deps.memgraph.io"


def test_no_file_s3():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD CSV FROM 's3://{BUCKET_NAME}/nodes_no_file.csv' WITH HEADER AS row CREATE (n:N {{id: row.id, name: row.name}})"
    print(f"Load query: {load_query}")
    with pytest.raises(DatabaseError):
        execute_and_fetch_all(cursor, load_query)


def test_file_exists_s3():
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, f"set database setting 'aws.region' to '{AWS_REGION}'")
    execute_and_fetch_all(cursor, f"set database setting 'aws.access_key' to '{AWS_ACCESS_KEY_ID}'")
    execute_and_fetch_all(cursor, f"set database setting 'aws.secret_key' to '{AWS_SECRET_ACCESS_KEY}'")
    execute_and_fetch_all(cursor, f"set database setting 'aws.endpoint_url' to '{AWS_ENDPOINT_URL}'")

    load_query = f"LOAD CSV FROM 's3://{BUCKET_NAME}/simple_nodes.csv' WITH HEADER AS row CREATE (n:N {{id: row.id, name: row.name}})"

    execute_and_fetch_all(cursor, load_query)
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_file_exists_http():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD CSV FROM '{AWS_ENDPOINT_URL}/{BUCKET_NAME}/simple_nodes.csv' WITH HEADER AS row CREATE (n:N {{id: row.id, name: row.name}})"
    print(f"Load query: {load_query}")

    execute_and_fetch_all(cursor, load_query)
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def main():
    file_name = "simple_nodes.csv"

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
    parquet_file = Path(__file__).parent / file_name

    if not parquet_file.exists():
        print(f"Error: File {parquet_file} does not exist")
        sys.exit(1)

    print(f"Uploading {parquet_file} to s3://{BUCKET_NAME}/{file_name}")
    s3_client.upload_file(str(parquet_file), BUCKET_NAME, file_name)
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
