from typing import Any, Dict

import elasticsearch

# Unit tests constants
CLUSTER_NAME = "cluster_name"
CLUSTER_UUID = "cluster_uuid"
NAME = "name"
TAGLINE = "tagline"
VERSION = "version"
BUILD_DATE = "build_date"
BUILD_FLAVOR = "build_flavor"
BUILD_HASH = "build_hash"
BUILD_SNAPSHOT = "build_snapshot"
BUILD_TYPE = "build_type"
LUCENE_VERSION = "lucene_version"
MINIMUM_INDEX_COMPATIBILITY_VERSION = "minimum_index_compatibility_version"
MINIMUM_WIRE_COMPATIBILITY_VERSION = "minimum_wire_compatibility_version"
NUMBER = "number"
ACKNOWLEDGED = "acknowledged"
INDEX = "index"
SHARDS_ACKNOWLEDGED = "shards_acknowledged"
# Connection constants
ELASTIC_URL = "elastic_url"
CA_CERTS = "ca_certs"
ELASTIC_USER = "elastic_user"
ELASTIC_PASSWORD = "elastic_password"
# Schema simple constants
SETTINGS = "settings"
NUMBER_OF_SHARDS = "number_of_shards"
NUMBER_OF_REPLICAS = "number_of_replicas"
MAPPINGS = "mappings"
IGNORE_MALFORMED = "ignore_malformed"


class SingletonMeta(type):
    """
    A metaclass approach to create a singleton design pattern. It is a class of class, or in other words a class is an instance of its metaclass. It gives us more control in the case we would need to customize the singleton class definitions in other ways. In this way, we avoid having a multiple inheritance.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ElasticSearchClientTest(elasticsearch.Elasticsearch, metaclass=SingletonMeta):
    """Singleton wrapper around elastic search client. Uses inheritance for implementing delegation."""

    def __init__(self, elastic_url: str, ca_certs: str, elastic_user: str, elastic_password: str) -> None:
        self.indices: IndicesClientTest = IndicesClientTest()

    def info(self) -> Dict[str, Any]:
        return {
            CLUSTER_NAME: "elasticsearch",
            CLUSTER_UUID: "HDFJBDDHF-993hurf",
            NAME: "elasticsearch-memgraph",
            TAGLINE: "You Know, for Search",
            VERSION: {
                BUILD_DATE: "2022-10-04T07:17:24.662462378Z",
                BUILD_FLAVOR: "default",
                BUILD_HASH: "42f05b9372a9a4a470db3b52817899b99a76ee73",
                BUILD_SNAPSHOT: False,
                BUILD_TYPE: "tar",
                LUCENE_VERSION: "9.3.0",
                MINIMUM_INDEX_COMPATIBILITY_VERSION: "7.0.0",
                MINIMUM_WIRE_COMPATIBILITY_VERSION: "7.17.0",
                NUMBER: "8.4.3",
            },
        }


class IndicesClientTest:
    def create(self, index: str, body: Dict[str, Any], ignore: int = 401) -> Dict[str, Any]:
        return {
            ACKNOWLEDGED: True,
            INDEX: "elasticsearch-memgraph-test",
            SHARDS_ACKNOWLEDGED: True,
        }
