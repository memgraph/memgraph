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
SNAPSHOT_FILE = "test_snapshot"


def test_file_exists_s3_with_config():
    cursor = connect(host="localhost", port=7687).cursor()

    load_query = f"RECOVER SNAPSHOT 's3://{BUCKET_NAME}/{SNAPSHOT_FILE}' WITH CONFIG {{'aws_region': '{AWS_REGION}', 'aws_access_key': '{AWS_ACCESS_KEY_ID}', 'aws_secret_key': '{AWS_SECRET_ACCESS_KEY}', 'aws_endpoint_url': '{AWS_ENDPOINT_URL}' }} FORCE"

    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_file_exists_no_s3_config():
    cursor = connect(host="localhost", port=7687).cursor()

    load_query = f"RECOVER SNAPSHOT 's3://{BUCKET_NAME}/{SNAPSHOT_FILE}' FORCE"
    try:
        execute_and_fetch_all(cursor, load_query)
    except DatabaseError as e:
        assert str(e) == "Failed to download snapshot file from s3 s3://deps.memgraph.io/test_snapshot"


def test_file_exists_s3_db_settings():
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, f"set database setting 'aws.region' to '{AWS_REGION}'")
    execute_and_fetch_all(cursor, f"set database setting 'aws.access_key' to '{AWS_ACCESS_KEY_ID}'")
    execute_and_fetch_all(cursor, f"set database setting 'aws.secret_key' to '{AWS_SECRET_ACCESS_KEY}'")
    execute_and_fetch_all(cursor, f"set database setting 'aws.endpoint_url' to '{AWS_ENDPOINT_URL}'")

    load_query = f"RECOVER SNAPSHOT 's3://{BUCKET_NAME}/{SNAPSHOT_FILE}' FORCE"

    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_http_file():
    cursor = connect(host="localhost", port=7687).cursor()
    SETTING_KEY = "file.download_conn_timeout_sec"
    setting_query = f"set database setting '{SETTING_KEY}' to '400'"
    execute_and_fetch_all(cursor, setting_query)
    res = dict(execute_and_fetch_all(cursor, "show database settings"))
    assert res[SETTING_KEY] == "400"
    load_query = f"RECOVER SNAPSHOT '{AWS_ENDPOINT_URL}/{BUCKET_NAME}/{SNAPSHOT_FILE}' FORCE"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_http_file_not_existing():
    cursor = connect(host="localhost", port=7687).cursor()
    SETTING_KEY = "file.download_conn_timeout_sec"
    setting_query = f"set database setting '{SETTING_KEY}' to '400'"
    execute_and_fetch_all(cursor, setting_query)
    res = dict(execute_and_fetch_all(cursor, "show database settings"))
    assert res[SETTING_KEY] == "400"
    try:
        load_query = f"RECOVER SNAPSHOT '{AWS_ENDPOINT_URL}/{BUCKET_NAME}/{SNAPSHOT_FILE}_why' FORCE"
    except DatabaseError as e:
        assert (
            str(e) == "Failed to download snapshot file from http://localhost:4566/deps.memgraph.io/test_snapshot_why"
        )


def prepare_file():
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

    # Upload remote snapshot file
    remote_path = Path(__file__).parent / SNAPSHOT_FILE

    if not remote_path.exists():
        print(f"Error: File {remote_path} does not exist")
        sys.exit(1)

    print(f"Uploading {remote_path} to s3://{BUCKET_NAME}/{SNAPSHOT_FILE}")
    s3_client.upload_file(str(remote_path), BUCKET_NAME, SNAPSHOT_FILE)
    print("Upload successful!")

    # List files in bucket to verify
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" in response:
        print(f"\nFiles in bucket '{BUCKET_NAME}':")
        for obj in response["Contents"]:
            print(f"  - s3://{BUCKET_NAME}/{obj['Key']} (Size: {obj['Size']} bytes)")


if __name__ == "__main__":
    prepare_file()
    sys.exit(pytest.main([__file__, "-rA"]))
