# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

from abc import ABC

import helpers
from benchmark_context import BenchmarkContext


# Base dataset class used as a template to create each individual dataset. All
# common logic is handled here.
# This dataset is used also for disk storage. In that case, we have two dataset files, one for nodes
# and the second one for edges. Managing index file is handled in the same way. If the workload is used
# for disk storage, when calling the get_file() method, the exception will be raised. The user has to use
# get_node_file() or get_edge_file() method to get the correct file path.
class Workload(ABC):
    # Name of the workload/dataset.
    NAME = ""
    # List of all variants of the workload/dataset that exist.
    VARIANTS = ["default"]
    # One of the available variants that should be used as the default variant.
    DEFAULT_VARIANT = "default"

    # List of local files that should be used to import the dataset.
    LOCAL_FILE = None
    LOCAL_FILE_NODES = None
    LOCAL_FILE_EDGES = None

    # URLs of remote dataset files that should be used to import the dataset, compressed in gz format.
    URL_FILE = None
    URL_FILE_NODES = None
    URL_FILE_EDGES = None

    # Index files
    LOCAL_INDEX_FILE = None
    URL_INDEX_FILE = None

    # Number of vertices/edges for each variant.
    SIZES = {
        "default": {"vertices": 0, "edges": 0},
    }

    # Indicates whether the dataset has properties on edges.
    PROPERTIES_ON_EDGES = False

    def __init_subclass__(cls) -> None:
        name_prerequisite = "NAME" in cls.__dict__
        generator_prerequisite = "dataset_generator" in cls.__dict__
        generator_indexes_prerequisite = "indexes_generator" in cls.__dict__
        custom_import_prerequisite = "custom_import" in cls.__dict__
        basic_import_prerequisite = (
            "LOCAL_FILE" in cls.__dict__
            or "URL_FILE" in cls.__dict__
            or ("URL_FILE_NODES" in cls.__dict__ and "URL_FILE_EDGES" in cls.__dict__)
        ) and ("LOCAL_INDEX_FILE" in cls.__dict__ or "URL_INDEX_FILE" in cls.__dict__)

        if not name_prerequisite:
            raise ValueError(
                """
                Can't define a workload class {} without NAME property:  NAME = "dataset name"
                Name property defines the workload you want to execute, for example: "demo/*/*/*"
                """.format(
                    cls.__name__
                )
            )

        if generator_prerequisite and (custom_import_prerequisite or basic_import_prerequisite):
            raise ValueError(
                """
                The workload class {} cannot have defined dataset import and generate dataset at
                the same time.
                """.format(
                    cls.__name__
                )
            )

        if not generator_prerequisite and (not custom_import_prerequisite and not basic_import_prerequisite):
            raise ValueError(
                """
                The workload class {} need to have defined dataset import or dataset generator
                """.format(
                    cls.__name__
                )
            )

        if generator_prerequisite and not generator_indexes_prerequisite:
            raise ValueError("The workload class {} need to define indexes_generator for generating a dataset. ")

        return super().__init_subclass__()

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None, disk_workload: bool = False):
        """
        Accepts a `variant` variable that indicates which variant
        of the dataset should be executed
        """
        self.benchmark_context = benchmark_context
        self._variant = variant
        self._vendor = benchmark_context.vendor
        self._file = None
        self._node_file = None
        self._edge_file = None
        self._file_index = None

        self.disk_workload: bool = disk_workload

        if self.NAME == "":
            raise ValueError("Give your workload a name, by setting self.NAME")

        self._validate_variant_argument()
        self._validate_vendor_argument()

        self._set_local_files()
        self._set_url_files()

        self._set_local_index_file()
        self._set_url_index_file()

        self._size = self.SIZES[variant]
        if "vertices" in self._size or "edges" in self._size:
            self._num_vertices = self._size["vertices"]
            self._num_edges = self._size["edges"]

    def _validate_variant_argument(self) -> None:
        if self._variant is None:
            variant = self.DEFAULT_VARIANT

        if self._variant not in self.VARIANTS:
            raise ValueError("Invalid test variant!")

        if self._variant not in self.SIZES:
            raise ValueError("The variant doesn't have a defined dataset " "size!")

        if not self.disk_workload:
            if (self.LOCAL_FILE and self._variant not in self.LOCAL_FILE) and (
                self.URL_FILE and self._variant not in self.URL_FILE
            ):
                raise ValueError("The variant doesn't have a defined URL or LOCAL file path!")
        else:
            if (self.LOCAL_FILE_NODES and self._variant not in self.LOCAL_FILE_NODES) and (
                self.URL_FILE_NODES and self._variant not in self.URL_FILE_NODES
            ):
                raise ValueError("The variant doesn't have a defined URL or LOCAL file path for nodes!")
            if (self.LOCAL_FILE_EDGES and self._variant not in self.LOCAL_FILE_EDGES) and (
                self.URL_FILE_EDGES and self._variant not in self.URL_FILE_EDGES
            ):
                raise ValueError("The variant doesn't have a defined URL or LOCAL file path for edges!")

    def _validate_vendor_argument(self) -> None:
        if (self.LOCAL_INDEX_FILE and self._vendor not in self.LOCAL_INDEX_FILE) and (
            self.URL_INDEX_FILE and self._vendor not in self.URL_INDEX_FILE
        ):
            raise ValueError("Vendor does not have INDEX for dataset!")

    def _set_local_files(self) -> None:
        if self.disk_workload and self._vendor != "neo4j":
            if self.LOCAL_FILE_NODES is not None:
                self._local_file_nodes = self.LOCAL_FILE_NODES.get(self._variant, None)
            else:
                self._local_file_nodes = None

            if self.LOCAL_FILE_EDGES is not None:
                self._local_file_edges = self.LOCAL_FILE_EDGES.get(self._variant, None)
            else:
                self._local_file_edges = None
        else:
            if self.LOCAL_FILE is not None:
                self._local_file = self.LOCAL_FILE.get(self._variant, None)
            else:
                self._local_file = None

    def _set_url_files(self) -> None:
        if self.disk_workload and self._vendor != "neo4j":
            if self.URL_FILE_NODES is not None:
                self._url_file_nodes = self.URL_FILE_NODES.get(self._variant, None)
            else:
                self._url_file_nodes = None
            if self.URL_FILE_EDGES is not None:
                self._url_file_edges = self.URL_FILE_EDGES.get(self._variant, None)
            else:
                self._url_file_edges = None
        else:
            if self.URL_FILE is not None:
                self._url_file = self.URL_FILE.get(self._variant, None)
            else:
                self._url_file = None

    def _set_local_index_file(self) -> None:
        if self.LOCAL_INDEX_FILE is not None:
            self._local_index = self.LOCAL_INDEX_FILE.get(self._vendor, None)
        else:
            self._local_index = None

    def _set_url_index_file(self) -> None:
        if self.URL_INDEX_FILE is not None:
            self._url_index = self.URL_INDEX_FILE.get(self._vendor, None)
        else:
            self._url_index = None

    def prepare(self, directory):
        if self.disk_workload and self._vendor != "neo4j":
            self._prepare_dataset_for_on_disk_workload(directory)
        else:
            self._prepare_dataset_for_in_memory_workload(directory)

        if self._local_index is not None:
            print("Using local index file:", self._local_index)
            self._file_index = self._local_index
        elif self._url_index is not None:
            cached_index, exists = directory.get_file(self._vendor + ".cypher")
            if not exists:
                print("Downloading index file:", self._url_index)
                downloaded_file = helpers.download_file(self._url_index, directory.get_path())
                print("Unpacking and caching file:", downloaded_file)
                helpers.unpack_gz_and_move_file(downloaded_file, cached_index)
            print("Using cached index file:", cached_index)
            self._file_index = cached_index

    def _prepare_dataset_for_on_disk_workload(self, directory):
        self._prepare_nodes_for_on_disk_workload(directory)
        self._prepare_edges_for_on_disk_workload(directory)

    def _prepare_nodes_for_on_disk_workload(self, directory):
        if self._local_file_nodes is not None:
            print("Using local dataset file for nodes:", self._local_file_nodes)
            self._node_file = self._local_file_nodes
        elif self._url_file_nodes is not None:
            cached_input, exists = directory.get_file("dataset_nodes.cypher")
            if not exists:
                print("Downloading dataset file for nodes:", self._url_file_nodes)
                downloaded_file = helpers.download_file(self._url_file_nodes, directory.get_path())
                print("Unpacking and caching file for nodes:", downloaded_file)
                helpers.unpack_gz_and_move_file(downloaded_file, cached_input)
            print("Using cached dataset file for nodes:", cached_input)
            self._node_file = cached_input

    def _prepare_edges_for_on_disk_workload(self, directory):
        if self._local_file_edges is not None:
            print("Using local dataset file for edges:", self._local_file_edges)
            self._edge_file = self._local_file_edges
        elif self._url_file_edges is not None:
            cached_input, exists = directory.get_file("dataset_edges.cypher")
            if not exists:
                print("Downloading dataset file for edges:", self._url_file_edges)
                downloaded_file = helpers.download_file(self._url_file_edges, directory.get_path())
                print("Unpacking and caching file for edges:", downloaded_file)
                helpers.unpack_gz_and_move_file(downloaded_file, cached_input)
            print("Using cached dataset file for edges:", cached_input)
            self._edge_file = cached_input

    def _prepare_dataset_for_in_memory_workload(self, directory):
        if self._local_file is not None:
            print("Using local dataset file:", self._local_file)
            self._file = self._local_file
        elif self._url_file is not None:
            cached_input, exists = directory.get_file("dataset.cypher")
            if not exists:
                print("Downloading dataset file:", self._url_file)
                downloaded_file = helpers.download_file(self._url_file, directory.get_path())
                print("Unpacking and caching file:", downloaded_file)
                helpers.unpack_gz_and_move_file(downloaded_file, cached_input)
            print("Using cached dataset file:", cached_input)
            self._file = cached_input

    def is_disk_workload(self):
        return self.disk_workload

    def get_variant(self):
        """Returns the current variant of the dataset."""
        return self._variant

    def get_index(self):
        """Get index file, defined by vendor"""
        return self._file_index

    def get_file(self):
        """
        Returns path to the file that contains dataset creation queries.
        """
        if self.disk_workload:
            raise Exception("get_file method should not be called for disk storage")

        return self._file

    def get_node_file(self):
        """Returns path to the file that contains dataset creation queries for nodes."""
        if not self.disk_workload:
            raise Exception("get_node_file method should be called only for disk storage")

        return self._node_file

    def get_edge_file(self):
        """Returns path to the file that contains dataset creation queries for edges."""
        if not self.disk_workload:
            raise Exception("get_edge_file method should be called only for disk storage")

        return self._edge_file

    def get_size(self):
        """Returns number of vertices/edges for the current variant."""
        return self._size

    def custom_import(self) -> bool:
        print("Workload does not have a custom import")
        return False

    def dataset_generator(self) -> list:
        print("Workload is not auto generated")
        return []

    # All tests should be query generator functions that output all of the
    # queries that should be executed by the runner. The functions should be
    # named `benchmark__GROUPNAME__TESTNAME` and should not accept any
    # arguments.
