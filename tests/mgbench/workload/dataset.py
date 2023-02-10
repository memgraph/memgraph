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

import helpers


# Base dataset class used as a template to create each individual dataset. All
# common logic is handled here.
class Dataset:
    # Name of the dataset.
    NAME = "Base dataset"
    # List of all variants of the dataset that exist.
    VARIANTS = ["default"]
    # One of the available variants that should be used as the default variant.
    DEFAULT_VARIANT = "default"
    # List of query files that should be used to import the dataset.
    FILES = {
        "default": "/foo/bar",
    }
    INDEX = None
    INDEX_FILES = {"default": ""}
    # List of query file URLs that should be used to import the dataset.
    URLS = None
    # Number of vertices/edges for each variant.
    SIZES = {
        "default": {"vertices": 0, "edges": 0},
    }
    # Indicates whether the dataset has properties on edges.
    PROPERTIES_ON_EDGES = False

    def __init__(self, variant=None, vendor=None):
        """
        Accepts a `variant` variable that indicates which variant
        of the dataset should be executed.
        """
        if variant is None:
            variant = self.DEFAULT_VARIANT
        if variant not in self.VARIANTS:
            raise ValueError("Invalid test variant!")
        if (self.FILES and variant not in self.FILES) and (self.URLS and variant not in self.URLS):
            raise ValueError("The variant doesn't have a defined URL or " "file path!")
        if variant not in self.SIZES:
            raise ValueError("The variant doesn't have a defined dataset " "size!")
        if vendor not in self.INDEX_FILES:
            raise ValueError("Vendor does not have INDEX for dataset!")
        self._variant = variant
        self._vendor = vendor
        if self.FILES is not None:
            self._file = self.FILES.get(variant, None)
        else:
            self._file = None
        if self.URLS is not None:
            self._url = self.URLS.get(variant, None)
        else:
            self._url = None

        if self.INDEX_FILES is not None:
            self._index = self.INDEX_FILES.get(vendor, None)
        else:
            self._index = None

        self._size = self.SIZES[variant]
        if "vertices" not in self._size or "edges" not in self._size:
            raise ValueError("The size defined for this variant doesn't " "have the number of vertices and/or edges!")
        self._num_vertices = self._size["vertices"]
        self._num_edges = self._size["edges"]

    def prepare(self, directory):
        if self._file is not None:
            print("Using dataset file:", self._file)
        else:
            # TODO: add support for JSON datasets
            cached_input, exists = directory.get_file("dataset.cypher")
            if not exists:
                print("Downloading dataset file:", self._url)
                downloaded_file = helpers.download_file(self._url, directory.get_path())
                print("Unpacking and caching file:", downloaded_file)
                helpers.unpack_and_move_file(downloaded_file, cached_input)
            print("Using cached dataset file:", cached_input)
            self._file = cached_input

        cached_index, exists = directory.get_file(self._vendor + ".cypher")
        if not exists:
            print("Downloading index file:", self._index)
            downloaded_file = helpers.download_file(self._index, directory.get_path())
            print("Unpacking and caching file:", downloaded_file)
            helpers.unpack_and_move_file(downloaded_file, cached_index)
        print("Using cached index file:", cached_index)
        self._index = cached_index

    def get_variant(self):
        """Returns the current variant of the dataset."""
        return self._variant

    def get_index(self):
        """Get index file, defined by vendor"""
        return self._index

    def get_file(self):
        """
        Returns path to the file that contains dataset creation queries.
        """
        return self._file

    def get_size(self):
        """Returns number of vertices/edges for the current variant."""
        return self._size

    # All tests should be query generator functions that output all of the
    # queries that should be executed by the runner. The functions should be
    # named `benchmark__GROUPNAME__TESTNAME` and should not accept any
    # arguments.
