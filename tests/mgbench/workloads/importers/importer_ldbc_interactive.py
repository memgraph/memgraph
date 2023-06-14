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

# --- DISCLAIMER: This is NOT an official implementation of an LDBC Benchmark.  ---
import csv
import subprocess
from pathlib import Path

import helpers
from benchmark_context import BenchmarkContext
from runners import BaseRunner

# Removed speaks/email from person header
HEADERS_INTERACTIVE = {
    "static/organisation": "id:ID(Organisation)|:LABEL|name:STRING|url:STRING",
    "static/place": "id:ID(Place)|name:STRING|url:STRING|:LABEL",
    "static/tagclass": "id:ID(TagClass)|name:STRING|url:STRING",
    "static/tag": "id:ID(Tag)|name:STRING|url:STRING",
    "static/tagclass_isSubclassOf_tagclass": ":START_ID(TagClass)|:END_ID(TagClass)",
    "static/tag_hasType_tagclass": ":START_ID(Tag)|:END_ID(TagClass)",
    "static/organisation_isLocatedIn_place": ":START_ID(Organisation)|:END_ID(Place)",
    "static/place_isPartOf_place": ":START_ID(Place)|:END_ID(Place)",
    "dynamic/comment": "id:ID(Comment)|creationDate:LOCALDATETIME|locationIP:STRING|browserUsed:STRING|content:STRING|length:INT",
    "dynamic/forum": "id:ID(Forum)|title:STRING|creationDate:LOCALDATETIME",
    "dynamic/person": "id:ID(Person)|firstName:STRING|lastName:STRING|gender:STRING|birthday:LOCALDATETIME|creationDate:LOCALDATETIME|locationIP:STRING|browserUsed:STRING",
    "dynamic/post": "id:ID(Post)|imageFile:STRING|creationDate:LOCALDATETIME|locationIP:STRING|browserUsed:STRING|language:STRING|content:STRING|length:INT",
    "dynamic/comment_hasCreator_person": ":START_ID(Comment)|:END_ID(Person)",
    "dynamic/comment_isLocatedIn_place": ":START_ID(Comment)|:END_ID(Place)",
    "dynamic/comment_replyOf_comment": ":START_ID(Comment)|:END_ID(Comment)",
    "dynamic/comment_replyOf_post": ":START_ID(Comment)|:END_ID(Post)",
    "dynamic/forum_containerOf_post": ":START_ID(Forum)|:END_ID(Post)",
    "dynamic/forum_hasMember_person": ":START_ID(Forum)|:END_ID(Person)|joinDate:LOCALDATETIME",
    "dynamic/forum_hasModerator_person": ":START_ID(Forum)|:END_ID(Person)",
    "dynamic/forum_hasTag_tag": ":START_ID(Forum)|:END_ID(Tag)",
    "dynamic/person_hasInterest_tag": ":START_ID(Person)|:END_ID(Tag)",
    "dynamic/person_isLocatedIn_place": ":START_ID(Person)|:END_ID(Place)",
    "dynamic/person_knows_person": ":START_ID(Person)|:END_ID(Person)|creationDate:LOCALDATETIME",
    "dynamic/person_likes_comment": ":START_ID(Person)|:END_ID(Comment)|creationDate:LOCALDATETIME",
    "dynamic/person_likes_post": ":START_ID(Person)|:END_ID(Post)|creationDate:LOCALDATETIME",
    "dynamic/person_studyAt_organisation": ":START_ID(Person)|:END_ID(Organisation)|classYear:INT",
    "dynamic/person_workAt_organisation": ":START_ID(Person)|:END_ID(Organisation)|workFrom:INT",
    "dynamic/post_hasCreator_person": ":START_ID(Post)|:END_ID(Person)",
    "dynamic/comment_hasTag_tag": ":START_ID(Comment)|:END_ID(Tag)",
    "dynamic/post_hasTag_tag": ":START_ID(Post)|:END_ID(Tag)",
    "dynamic/post_isLocatedIn_place": ":START_ID(Post)|:END_ID(Place)",
}


class ImporterLDBCInteractive:
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
            print("Running Neo4j import")
            dump_dir = Path() / ".cache" / "datasets" / self._dataset_name / self._variant / "dump"
            dump_dir.mkdir(parents=True, exist_ok=True)
            dir_name = self._csv_dict[self._variant].split("/")[-1:][0].replace(".tar.zst", "")
            if (dump_dir / dir_name).exists() and any((dump_dir / dir_name).iterdir()):
                print("Files downloaded")
                dump_dir = dump_dir / dir_name
            else:
                print("Downloading files")
                downloaded_file = helpers.download_file(self._csv_dict[self._variant], dump_dir.absolute())
                print("Unpacking the file..." + downloaded_file)
                dump_dir = helpers.unpack_tar_zst(Path(downloaded_file))

            input_files = {}
            for file in dump_dir.glob("*/*0.csv"):
                parts = file.parts[-2:]
                key = parts[0] + "/" + parts[1][:-8]
                input_files[key] = file

            output_files = {}
            for key, file in input_files.items():
                output = file.parent / (file.stem + "_neo" + ".csv")
                if not output.exists():
                    with file.open("r") as input_f, output.open("a") as output_f:
                        reader = csv.reader(input_f, delimiter="|")
                        header = next(reader)

                        writer = csv.writer(output_f, delimiter="|")
                        if key in HEADERS_INTERACTIVE.keys():
                            updated_header = HEADERS_INTERACTIVE[key].split("|")
                            writer.writerow(updated_header)
                        for line in reader:
                            if "creationDate" in header:
                                pos = header.index("creationDate")
                                line[pos] = line[pos][0:-5]
                            elif "joinDate" in header:
                                pos = header.index("joinDate")
                                line[pos] = line[pos][0:-5]

                            if "organisation_0_0.csv" == file.name:
                                writer.writerow([line[0], line[1].capitalize(), line[2], line[3]])
                            elif "place_0_0.csv" == file.name:
                                writer.writerow([line[0], line[1], line[2], line[3].capitalize()])
                            else:
                                writer.writerow(line)

                output_files[key] = output.as_posix()
            vendor_runner.clean_db()
            subprocess.run(
                args=[
                    vendor_runner._neo4j_admin,
                    "database",
                    "import",
                    "full",
                    "--id-type=INTEGER",
                    "--nodes=Place=" + output_files["static/place"],
                    "--nodes=Organisation=" + output_files["static/organisation"],
                    "--nodes=TagClass=" + output_files["static/tagclass"],
                    "--nodes=Tag=" + output_files["static/tag"],
                    "--nodes=Comment:Message=" + output_files["dynamic/comment"],
                    "--nodes=Forum=" + output_files["dynamic/forum"],
                    "--nodes=Person=" + output_files["dynamic/person"],
                    "--nodes=Post:Message=" + output_files["dynamic/post"],
                    "--relationships=IS_PART_OF=" + output_files["static/place_isPartOf_place"],
                    "--relationships=IS_SUBCLASS_OF=" + output_files["static/tagclass_isSubclassOf_tagclass"],
                    "--relationships=IS_LOCATED_IN=" + output_files["static/organisation_isLocatedIn_place"],
                    "--relationships=HAS_TYPE=" + output_files["static/tag_hasType_tagclass"],
                    "--relationships=HAS_CREATOR=" + output_files["dynamic/comment_hasCreator_person"],
                    "--relationships=IS_LOCATED_IN=" + output_files["dynamic/comment_isLocatedIn_place"],
                    "--relationships=REPLY_OF=" + output_files["dynamic/comment_replyOf_comment"],
                    "--relationships=REPLY_OF=" + output_files["dynamic/comment_replyOf_post"],
                    "--relationships=CONTAINER_OF=" + output_files["dynamic/forum_containerOf_post"],
                    "--relationships=HAS_MEMBER=" + output_files["dynamic/forum_hasMember_person"],
                    "--relationships=HAS_MODERATOR=" + output_files["dynamic/forum_hasModerator_person"],
                    "--relationships=HAS_TAG=" + output_files["dynamic/forum_hasTag_tag"],
                    "--relationships=HAS_INTEREST=" + output_files["dynamic/person_hasInterest_tag"],
                    "--relationships=IS_LOCATED_IN=" + output_files["dynamic/person_isLocatedIn_place"],
                    "--relationships=KNOWS=" + output_files["dynamic/person_knows_person"],
                    "--relationships=LIKES=" + output_files["dynamic/person_likes_comment"],
                    "--relationships=LIKES=" + output_files["dynamic/person_likes_post"],
                    "--relationships=HAS_CREATOR=" + output_files["dynamic/post_hasCreator_person"],
                    "--relationships=HAS_TAG=" + output_files["dynamic/comment_hasTag_tag"],
                    "--relationships=HAS_TAG=" + output_files["dynamic/post_hasTag_tag"],
                    "--relationships=IS_LOCATED_IN=" + output_files["dynamic/post_isLocatedIn_place"],
                    "--relationships=STUDY_AT=" + output_files["dynamic/person_studyAt_organisation"],
                    "--relationships=WORK_AT=" + output_files["dynamic/person_workAt_organisation"],
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
