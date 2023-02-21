import csv
import subprocess
from collections import defaultdict
from pathlib import Path

import helpers
from runners import Client, Memgraph, Neo4j, Runners
from workload.dataset import Dataset

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

HEADERS_URL = "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/headers.tar.gz"


class Importer:
    def __init__(self, dataset: Dataset, vendor: Runners, client: Client):
        self._dataset = dataset
        self._vendor = vendor
        self._size = dataset.get_variant()
        self._client = client

    def try_optimal_import(self) -> bool:
        if self._dataset.NAME == "ldbc_interactive" and isinstance(self._vendor, Neo4j):
            print("Runnning Neo4j import")
            dump_dir = Path() / ".cache" / "datasets" / self._dataset.NAME / self._size / "dump"
            dump_dir.mkdir(exist_ok=True)
            dir_name = self._dataset.URL_CSV[self._size].split("/")[-1:][0].removesuffix(".tar.zst")
            if (dump_dir / dir_name).exists():
                print("Files downloaded")
                dump_dir = dump_dir / dir_name
            else:
                print("Downloading files")
                downloaded_file = helpers.download_file(self._dataset.URL_CSV[self._size], dump_dir.absolute())
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
            self._vendor.clean_db()
            subprocess.run(
                args=[
                    self._vendor._neo4j_admin,
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

            self._vendor.start_preparation("Index preparation")
            print("Executing database index setup")
            self._client.execute(file_path=self._dataset.get_index(), num_workers=1)
            self._vendor.stop("Stop index preparation")

            return True

        elif self._dataset.NAME == "ldbc_interactive" and isinstance(self._vendor, Memgraph):

            self._vendor.start_preparation("import")
            print("Executing database cleanup and index setup...")
            ret = self._client.execute(
                queries=[
                    ("MATCH (n) DETACH DELETE n;", {}),
                ],
                num_workers=1,
            )

            ret = self._client.execute(file_path=self._dataset.get_index(), num_workers=12)
            print("Importing dataset...")
            ret = self._client.execute(file_path=self._dataset.get_file_cypherl(), num_workers=12, max_retries=400)
            usage = self._vendor.stop("import")
            for row in ret:
                print(
                    "Executed",
                    row["count"],
                    "queries in",
                    row["duration"],
                    "seconds using",
                    row["num_workers"],
                    "workers with a total throughput of",
                    row["throughput"],
                    "queries/second.",
                )
                print()
                print(
                    "The database used",
                    usage["cpu"],
                    "seconds of CPU time and peaked at",
                    usage["memory"] / 1024 / 1024,
                    "MiB of RAM.",
                )

            return True

        elif self._dataset.NAME == "ldbc_bi" and isinstance(self._vendor, Neo4j):

            print("Runnning Neo4j import")
            data_dir = Path() / ".cache" / "datasets" / self._dataset.NAME / self._size / "data_neo4j"
            data_dir.mkdir(exist_ok=True)
            dir_name = self._dataset.URL_CSV[self._size].split("/")[-1:][0].removesuffix(".tar.zst")
            if (data_dir / dir_name).exists():
                print("Files downloaded")
                data_dir = data_dir / dir_name
            else:
                print("Downloading files")
                downloaded_file = helpers.download_file(self._dataset.URL_CSV[self._size], data_dir.absolute())
                print("Unpacking the file..." + downloaded_file)
                data_dir = helpers.unpack_tar_zst(Path(downloaded_file))

            headers_dir = Path() / ".cache" / "datasets" / self._dataset.NAME / self._size / "headers_neo4j"
            headers_dir.mkdir(exist_ok=True)
            headers = HEADERS_URL.split("/")[-1:][0].removesuffix(".tar.gz")
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

            self._vendor.clean_db()
            subprocess.run(
                args=[
                    self._vendor._neo4j_admin,
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

            self._vendor.start_preparation("Index preparation")
            print("Executing database index setup")
            self._client.execute(file_path=self._dataset.get_index(), num_workers=1)
            self._vendor.stop("Stop index preparation")

            return True

        elif self._dataset.NAME == "ldbc_bi" and isinstance(self._vendor, Memgraph):

            self._vendor.start_preparation("import")
            print("Executing database cleanup and index setup...")
            ret = self._client.execute(
                queries=[
                    ("MATCH (n) DETACH DELETE n;", {}),
                ],
                num_workers=1,
            )

            ret = self._client.execute(file_path=self._dataset.get_index(), num_workers=1)
            print("Importing dataset...")
            ret = self._client.execute(file_path=self._dataset.get_file_cypherl(), num_workers=12, max_retries=400)
            usage = self._vendor.stop("import")
            for row in ret:
                print(
                    "Executed",
                    row["count"],
                    "queries in",
                    row["duration"],
                    "seconds using",
                    row["num_workers"],
                    "workers with a total throughput of",
                    row["throughput"],
                    "queries/second.",
                )
                print()
                print(
                    "The database used",
                    usage["cpu"],
                    "seconds of CPU time and peaked at",
                    usage["memory"] / 1024 / 1024,
                    "MiB of RAM.",
                )
                return True

        else:
            return False

    def bi_memgraph():
        pass
