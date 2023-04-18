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

# ---  DISCLAIMER: This is NOT an official implementation of an LDBC Benchmark. ---
import csv
import subprocess
from collections import defaultdict
from pathlib import Path

import helpers
from benchmark_context import BenchmarkContext
from runners import BaseRunner

HEADERS_URL = "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/headers.tar.gz"


class ImporterLDBCBI:
    def __init__(
        self, benchmark_context: BenchmarkContext, dataset_name: str, variant: str, index_file: str, csv_dict: dict
    ) -> None:
        self._benchmark_context = benchmark_context
        self._dataset_name = dataset_name
        self._variant = variant
        self._index_file = index_file
        self._csv_dict = csv_dict

    def execute_import(self):
        vendor_runner = BaseRunner.create(
            benchmark_context=self._benchmark_context,
        )
        client = vendor_runner.fetch_client()

        if self._benchmark_context.vendor_name == "neo4j":
            data_dir = Path() / ".cache" / "datasets" / self._dataset_name / self._variant / "data_neo4j"
            data_dir.mkdir(parents=True, exist_ok=True)
            dir_name = self._csv_dict[self._variant].split("/")[-1:][0].replace(".tar.zst", "")
            if (data_dir / dir_name).exists() and any((data_dir / dir_name).iterdir()):
                print("Files downloaded")
                data_dir = data_dir / dir_name
            else:
                print("Downloading files")
                downloaded_file = helpers.download_file(self._csv_dict[self._variant], data_dir.absolute())
                print("Unpacking the file..." + downloaded_file)
                data_dir = helpers.unpack_tar_zst(Path(downloaded_file))

            headers_dir = Path() / ".cache" / "datasets" / self._dataset_name / self._variant / "headers_neo4j"
            headers_dir.mkdir(parents=True, exist_ok=True)
            headers = HEADERS_URL.split("/")[-1:][0].replace(".tar.gz", "")
            if (headers_dir / headers).exists():
                print("Header files downloaded.")
            else:
                print("Downloading files")
                downloaded_file = helpers.download_file(HEADERS_URL, headers_dir.absolute())
                print("Unpacking the file..." + downloaded_file)
                headers_dir = helpers.unpack_tar_gz(Path(downloaded_file))

            input_headers = {}
            for header_file in headers_dir.glob("**/*.csv"):
                key = "/".join(header_file.parts[-2:])[0:-4]
                input_headers[key] = header_file.as_posix()

            for data_file in data_dir.glob("**/*.gz"):
                if "initial_snapshot" in data_file.parts:
                    data_file = helpers.unpack_gz(data_file)
                    output = data_file.parent / (data_file.stem + "_neo" + ".csv")
                    if not output.exists():
                        with data_file.open("r") as input_f, output.open("a") as output_f:
                            reader = csv.reader(input_f, delimiter="|")
                            header = next(reader)
                            writer = csv.writer(output_f, delimiter="|")
                            for line in reader:
                                writer.writerow(line)
                    else:
                        print("Files converted")

            input_files = defaultdict(list)
            for neo_file in data_dir.glob("**/*_neo.csv"):
                key = "/".join(neo_file.parts[-3:-1])
                input_files[key].append(neo_file.as_posix())

            vendor_runner.clean_db()
            subprocess.run(
                args=[
                    vendor_runner._neo4j_admin,
                    "database",
                    "import",
                    "full",
                    "--id-type=INTEGER",
                    "--ignore-empty-strings=true",
                    "--bad-tolerance=0",
                    "--nodes=Place=" + input_headers["static/Place"] + "," + ",".join(input_files["static/Place"]),
                    "--nodes=Organisation="
                    + input_headers["static/Organisation"]
                    + ","
                    + ",".join(input_files["static/Organisation"]),
                    "--nodes=TagClass="
                    + input_headers["static/TagClass"]
                    + ","
                    + ",".join(input_files["static/TagClass"]),
                    "--nodes=Tag=" + input_headers["static/Tag"] + "," + ",".join(input_files["static/Tag"]),
                    "--nodes=Forum=" + input_headers["dynamic/Forum"] + "," + ",".join(input_files["dynamic/Forum"]),
                    "--nodes=Person=" + input_headers["dynamic/Person"] + "," + ",".join(input_files["dynamic/Person"]),
                    "--nodes=Message:Comment="
                    + input_headers["dynamic/Comment"]
                    + ","
                    + ",".join(input_files["dynamic/Comment"]),
                    "--nodes=Message:Post="
                    + input_headers["dynamic/Post"]
                    + ","
                    + ",".join(input_files["dynamic/Post"]),
                    "--relationships=IS_PART_OF="
                    + input_headers["static/Place_isPartOf_Place"]
                    + ","
                    + ",".join(input_files["static/Place_isPartOf_Place"]),
                    "--relationships=IS_SUBCLASS_OF="
                    + input_headers["static/TagClass_isSubclassOf_TagClass"]
                    + ","
                    + ",".join(input_files["static/TagClass_isSubclassOf_TagClass"]),
                    "--relationships=IS_LOCATED_IN="
                    + input_headers["static/Organisation_isLocatedIn_Place"]
                    + ","
                    + ",".join(input_files["static/Organisation_isLocatedIn_Place"]),
                    "--relationships=HAS_TYPE="
                    + input_headers["static/Tag_hasType_TagClass"]
                    + ","
                    + ",".join(input_files["static/Tag_hasType_TagClass"]),
                    "--relationships=HAS_CREATOR="
                    + input_headers["dynamic/Comment_hasCreator_Person"]
                    + ","
                    + ",".join(input_files["dynamic/Comment_hasCreator_Person"]),
                    "--relationships=IS_LOCATED_IN="
                    + input_headers["dynamic/Comment_isLocatedIn_Country"]
                    + ","
                    + ",".join(input_files["dynamic/Comment_isLocatedIn_Country"]),
                    "--relationships=REPLY_OF="
                    + input_headers["dynamic/Comment_replyOf_Comment"]
                    + ","
                    + ",".join(input_files["dynamic/Comment_replyOf_Comment"]),
                    "--relationships=REPLY_OF="
                    + input_headers["dynamic/Comment_replyOf_Post"]
                    + ","
                    + ",".join(input_files["dynamic/Comment_replyOf_Post"]),
                    "--relationships=CONTAINER_OF="
                    + input_headers["dynamic/Forum_containerOf_Post"]
                    + ","
                    + ",".join(input_files["dynamic/Forum_containerOf_Post"]),
                    "--relationships=HAS_MEMBER="
                    + input_headers["dynamic/Forum_hasMember_Person"]
                    + ","
                    + ",".join(input_files["dynamic/Forum_hasMember_Person"]),
                    "--relationships=HAS_MODERATOR="
                    + input_headers["dynamic/Forum_hasModerator_Person"]
                    + ","
                    + ",".join(input_files["dynamic/Forum_hasModerator_Person"]),
                    "--relationships=HAS_TAG="
                    + input_headers["dynamic/Forum_hasTag_Tag"]
                    + ","
                    + ",".join(input_files["dynamic/Forum_hasTag_Tag"]),
                    "--relationships=HAS_INTEREST="
                    + input_headers["dynamic/Person_hasInterest_Tag"]
                    + ","
                    + ",".join(input_files["dynamic/Person_hasInterest_Tag"]),
                    "--relationships=IS_LOCATED_IN="
                    + input_headers["dynamic/Person_isLocatedIn_City"]
                    + ","
                    + ",".join(input_files["dynamic/Person_isLocatedIn_City"]),
                    "--relationships=KNOWS="
                    + input_headers["dynamic/Person_knows_Person"]
                    + ","
                    + ",".join(input_files["dynamic/Person_knows_Person"]),
                    "--relationships=LIKES="
                    + input_headers["dynamic/Person_likes_Comment"]
                    + ","
                    + ",".join(input_files["dynamic/Person_likes_Comment"]),
                    "--relationships=LIKES="
                    + input_headers["dynamic/Person_likes_Post"]
                    + ","
                    + ",".join(input_files["dynamic/Person_likes_Post"]),
                    "--relationships=HAS_CREATOR="
                    + input_headers["dynamic/Post_hasCreator_Person"]
                    + ","
                    + ",".join(input_files["dynamic/Post_hasCreator_Person"]),
                    "--relationships=HAS_TAG="
                    + input_headers["dynamic/Comment_hasTag_Tag"]
                    + ","
                    + ",".join(input_files["dynamic/Comment_hasTag_Tag"]),
                    "--relationships=HAS_TAG="
                    + input_headers["dynamic/Post_hasTag_Tag"]
                    + ","
                    + ",".join(input_files["dynamic/Post_hasTag_Tag"]),
                    "--relationships=IS_LOCATED_IN="
                    + input_headers["dynamic/Post_isLocatedIn_Country"]
                    + ","
                    + ",".join(input_files["dynamic/Post_isLocatedIn_Country"]),
                    "--relationships=STUDY_AT="
                    + input_headers["dynamic/Person_studyAt_University"]
                    + ","
                    + ",".join(input_files["dynamic/Person_studyAt_University"]),
                    "--relationships=WORK_AT="
                    + input_headers["dynamic/Person_workAt_Company"]
                    + ","
                    + ",".join(input_files["dynamic/Person_workAt_Company"]),
                    "--delimiter",
                    "|",
                    "neo4j",
                ],
                check=True,
            )

            vendor_runner.start_db_init("Index preparation")
            print("Executing database index setup")
            client.execute(file_path=self._index_file, num_workers=1)
            vendor_runner.stop_db_init("Stop index preparation")
            return True
        else:
            return False
