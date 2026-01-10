from typing import Any, Dict

import pytest
from mage.elastic_search_serialization.elastic_search_util import (
    ACKNOWLEDGED,
    BUILD_DATE,
    BUILD_FLAVOR,
    BUILD_HASH,
    BUILD_SNAPSHOT,
    BUILD_TYPE,
    CA_CERTS,
    CLUSTER_NAME,
    CLUSTER_UUID,
    ELASTIC_PASSWORD,
    ELASTIC_URL,
    ELASTIC_USER,
    IGNORE_MALFORMED,
    INDEX,
    LUCENE_VERSION,
    MAPPINGS,
    MINIMUM_INDEX_COMPATIBILITY_VERSION,
    MINIMUM_WIRE_COMPATIBILITY_VERSION,
    NAME,
    NUMBER,
    NUMBER_OF_REPLICAS,
    NUMBER_OF_SHARDS,
    SETTINGS,
    SHARDS_ACKNOWLEDGED,
    TAGLINE,
    VERSION,
    ElasticSearchClientTest,
)


@pytest.fixture
def connection_config() -> Dict[str, str]:
    return {
        ELASTIC_URL: "localhost",
        CA_CERTS: "path_to_certs_file",
        ELASTIC_USER: "memgraph",
        ELASTIC_PASSWORD: "<env_password>",
    }


@pytest.fixture
def schema_config() -> Dict[str, Any]:
    return {
        SETTINGS: {
            INDEX: {
                NUMBER_OF_SHARDS: 2,
                NUMBER_OF_REPLICAS: 3,
                MAPPINGS: {IGNORE_MALFORMED: True},
            }
        }
    }


@pytest.fixture
def index_config() -> str:
    return "test-index"


def test_connect(connection_config: Dict[str, str]) -> None:
    """Tests connection to the mock Elasticsearch system.
    Args:
        config (Dict[str, str]): Configuration info for connecting to the client.
    """
    client = ElasticSearchClientTest(
        connection_config[ELASTIC_URL],
        connection_config[CA_CERTS],
        connection_config[ELASTIC_USER],
        connection_config[ELASTIC_PASSWORD],
    )
    client_info = client.info()
    assert client_info[CLUSTER_NAME] == "elasticsearch"
    assert client_info[CLUSTER_UUID] == "HDFJBDDHF-993hurf"
    assert client_info[NAME] == "elasticsearch-memgraph"
    assert client_info[TAGLINE] == "You Know, for Search"
    assert client_info[VERSION][BUILD_DATE] == "2022-10-04T07:17:24.662462378Z"
    assert client_info[VERSION][BUILD_FLAVOR] == "default"
    assert client_info[VERSION][BUILD_HASH] == "42f05b9372a9a4a470db3b52817899b99a76ee73"
    assert client_info[VERSION][BUILD_SNAPSHOT] is False
    assert client_info[VERSION][BUILD_TYPE] == "tar"
    assert client_info[VERSION][LUCENE_VERSION] == "9.3.0"
    assert client_info[VERSION][MINIMUM_INDEX_COMPATIBILITY_VERSION] == "7.0.0"
    assert client_info[VERSION][MINIMUM_WIRE_COMPATIBILITY_VERSION] == "7.17.0"
    assert client_info[VERSION][NUMBER] == "8.4.3"


def test_create_index(connection_config: Dict[str, Any], schema_config: Dict[str, Any], index_config: str) -> None:
    """Tests creation of the index for the mocked Elasticsearch system.
    Args:
        config (Dict[str, str]): Configuration info for connecting to the client.
    """
    client = ElasticSearchClientTest(
        connection_config[ELASTIC_URL],
        connection_config[CA_CERTS],
        connection_config[ELASTIC_USER],
        connection_config[ELASTIC_PASSWORD],
    )
    response = client.indices.create(index=index_config, body=schema_config)
    assert response[ACKNOWLEDGED] is True
    assert response[INDEX] == "elasticsearch-memgraph-test"
    assert response[SHARDS_ACKNOWLEDGED] is True
