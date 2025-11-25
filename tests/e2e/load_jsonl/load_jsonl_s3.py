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
from common import connect, execute_and_fetch_all, get_file_path
from mgclient import DatabaseError

# LocalStack configuration
# NOTE: If you are testing this locally change localstack-s3 to localhost
AWS_ENDPOINT_URL = "http://localstack-s3:4566"
# AWS_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"
AWS_REGION = "us-east-1"
BUCKET_NAME = "deps.memgraph.io"


def test_invalid_setting():
    cursor = connect(host="localhost", port=7687).cursor()
    SETTING_KEY = "file.download_conn_timeout_sec"
    setting_query = f"set database setting '{SETTING_KEY}' to '-400'"
    # Should throw exception because of -400
    with pytest.raises(DatabaseError):
        execute_and_fetch_all(cursor, setting_query)

    setting_query = f"set database setting '{SETTING_KEY}' to 'abc'"
    # Should throw exception because of abc
    with pytest.raises(DatabaseError):
        execute_and_fetch_all(cursor, setting_query)


def test_http_file():
    cursor = connect(host="localhost", port=7687).cursor()
    SETTING_KEY = "file.download_conn_timeout_sec"
    setting_query = f"set database setting '{SETTING_KEY}' to '400'"
    execute_and_fetch_all(cursor, setting_query)
    res = dict(execute_and_fetch_all(cursor, "show database settings"))
    assert res[SETTING_KEY] == "400"
    load_query = f"LOAD JSONL FROM '{AWS_ENDPOINT_URL}/{BUCKET_NAME}/test_types.jsonl' AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 120
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

    # Upload JSONL file
    jsonl_file = Path(__file__).parent / "test_types.jsonl"

    if not jsonl_file.exists():
        print(f"Error: File {jsonl_file} does not exist")
        sys.exit(1)

    print(f"Uploading {jsonl_file} to s3://{BUCKET_NAME}/test_types.jsonl")
    s3_client.upload_file(str(jsonl_file), BUCKET_NAME, "test_types.jsonl")
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
