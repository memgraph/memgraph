# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

from abc import ABC, abstractclassmethod
from pathlib import Path

import helpers
from benchmark import BenchmarkContext


# Base dataset class used as a template to create each individual dataset. All
# common logic is handled here.
class Workload(ABC):
    # Name of the workload/dataset.
    NAME = "Base workload"
    # List of all variants of the workload/dataset that exist.
    VARIANTS = ["default"]
    # One of the available variants that should be used as the default variant.
    DEFAULT_VARIANT = "default"

    # List of local files that should be used to import the dataset.
    LOCAL_FILE = None

    # URLs of remote dataset files that should be used to import the dataset, compresed in gz format.
    URL_FILE = None

    # Index files
    LOCAL_INDEX_FILE = None
    URL_INDEX_FILE = None

    # Number of vertices/edges for each variant.
    SIZES = {
        "default": {"vertices": 0, "edges": 0},
    }

    # Indicates whether the dataset has properties on edges.
    PROPERTIES_ON_EDGES = False

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        """
        Accepts a `variant` variable that indicates which variant
        of the dataset should be executed
        """
        self.benchmark_context = benchmark_context
        self._variant = variant
        self._vendor = benchmark_context.vendor_name

        if variant is None:
            variant = self.DEFAULT_VARIANT
        if variant not in self.VARIANTS:
            raise ValueError("Invalid test variant!")
        if (self.LOCAL_FILE and variant not in self.LOCAL_FILE) and (self.URL_FILE and variant not in self.URL_FILE):
            raise ValueError("The variant doesn't have a defined URL or LOCAL file path!")
        if variant not in self.SIZES:
            raise ValueError("The variant doesn't have a defined dataset " "size!")

        if (self.LOCAL_INDEX_FILE and self._vendor not in self.LOCAL_INDEX_FILES) and (
            self.URL_INDEX_FILE and self._vendor not in self.URL_INDEX_FILES
        ):
            raise ValueError("Vendor does not have INDEX for dataset!")

        if self.LOCAL_FILE is not None:
            self._local_file = self.LOCAL_FILE.get(variant, None)
        else:
            self._local_file = None

        if self.URL_FILE is not None:
            self._url_file = self.URL_FILE.get(variant, None)
        else:
            self._url_file = None

        if self.LOCAL_INDEX_FILE is not None:
            self._local_index = self.LOCAL_INDEX_FILE.get(self._vendor, None)
        else:
            self._local_index = None

        if self.URL_INDEX_FILE is not None:
            self._url_index = self.URL_INDEX_FILE.get(self._vendor, None)
        else:
            self._url_index = None

        self._size = self.SIZES[variant]
        if "vertices" not in self._size or "edges" not in self._size:
            raise ValueError("The size defined for this variant doesn't have the number of vertices and/or edges!")
        self._num_vertices = self._size["vertices"]
        self._num_edges = self._size["edges"]

    def prepare(self, directory):
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
            self._file_ = cached_input

        if self._local_index is not None:
            print("Using local index file:", self._local_index)
            self._file_index = self._local_index
        elif self._url_index is not None:
            cached_index, exists = directory.get_file(self._vendor + ".cypher")
            if not exists:
                print("Downloading index file:", self._index)
                downloaded_file = helpers.download_file(self._index, directory.get_path())
                print("Unpacking and caching file:", downloaded_file)
                helpers.unpack_gz_and_move_file(downloaded_file, cached_index)
            print("Using cached index file:", cached_index)
            self._file_index = cached_index

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
        return self._file

    def get_size(self):
        """Returns number of vertices/edges for the current variant."""
        return self._size

    @abstractclassmethod
    def custom_import(self) -> bool:
        pass

    # All tests should be query generator functions that output all of the
    # queries that should be executed by the runner. The functions should be
    # named `benchmark__GROUPNAME__TESTNAME` and should not accept any
    # arguments.
