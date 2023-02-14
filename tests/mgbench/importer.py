import csv
import subprocess
from pathlib import Path

import helpers
from runners import Memgraph, Neo4j, Runners
from workload.dataset import Dataset

# Removed speaks/email
HEADERS_INTERACTIVE = {
    "static/organisation": "id:ID(Organisation)|:LABEL|name:STRING|url:STRING",
    "static/place": "id:ID(Place)|name:STRING|url:STRING|:LABEL",
    "static/tagclass": "id:ID(TagClass)|name:STRING|url:STRING",
    "static/tag": "id:ID(Tag)|name:STRING|url:STRING",
    "static/tagclass_isSubclassOf_tagclass": "START_ID(TagClass)|:END_ID(TagClass)",
    "static/tag_hasType_tagclass": ":START_ID(Tag)|:END_ID(TagClass)",
    "static/organisation_isLocatedIn_place": ":START_ID(Organisation)|:END_ID(Place)",
    "static/place_isPartOf_place": ":START_ID(Place)|:END_ID(Place)",
    "dynamic/comment": "id:ID(Comment)|creationDate:LONG|locationIP:STRING|browserUsed:STRING|content:STRING|length:INT",
    "dynamic/forum": "id:ID(Forum)|title:STRING|creationDate:LONG",
    "dynamic/person": "id:ID(Person)|firstName:STRING|lastName:STRING|gender:STRING|birthday:LONG|creationDate:LONG|locationIP:STRING|browserUsed:STRING",
    "dynamic/post": "id:ID(Post)|imageFile:STRING|creationDate:LONG|locationIP:STRING|browserUsed:STRING|language:STRING|content:STRING|length:INT",
    "dynamic/comment_hasCreator_person": ":START_ID(Comment)|:END_ID(Person)",
    "dynamic/comment_isLocatedIn_place": ":START_ID(Comment)|:END_ID(Place)",
    "dynamic/comment_replyOf_comment": ":START_ID(Comment)|:END_ID(Comment)",
    "dynamic/comment_replyOf_post": ":START_ID(Comment)|:END_ID(Post)",
    "dynamic/forum_containerOf_post": ":START_ID(Forum)|:END_ID(Post)",
    "dynamic/forum_hasMember_person": ":START_ID(Forum)|:END_ID(Person)|joinDate:LONG",
    "dynamic/forum_hasModerator_person": ":START_ID(Forum)|:END_ID(Person)",
    "dynamic/forum_hasTag_tag": ":START_ID(Forum)|:END_ID(Tag)",
    "dynamic/person_hasInterest_tag": ":START_ID(Person)|:END_ID(Tag)",
    "dynamic/person_isLocatedIn_place": ":START_ID(Person)|:END_ID(Place)",
    "dynamic/person_knows_person": ":START_ID(Person)|:END_ID(Person)|creationDate:LONG",
    "dynamic/person_likes_comment": ":START_ID(Person)|:END_ID(Comment)|creationDate:LONG",
    "dynamic/person_likes_post": ":START_ID(Person)|:END_ID(Post)|creationDate:LONG",
    "dynamic/person_studyAt_organisation": ":START_ID(Person)|:END_ID(Organisation)|classYear:INT",
    "dynamic/person_workAt_organisation": ":START_ID(Person)|:END_ID(Organisation)|workFrom:INT",
    "dynamic/post_hasCreator_person": ":START_ID(Post)|:END_ID(Person)",
    "dynamic/comment_hasTag_tag": ":START_ID(Comment)|:END_ID(Tag)",
    "dynamic/post_hasTag_tag": ":START_ID(Post)|:END_ID(Tag)",
    "dynamic/post_isLocatedIn_place": ":START_ID(Post)|:END_ID(Place)",
}


class Importer:
    def __init__(self, dataset: Dataset, vendor: Runners, size: str) -> bool:
        if dataset.NAME == "ldbc_interactive" and isinstance(vendor, Neo4j):
            print("Runnning Neo4j import")
            dump_dir = Path() / ".cache" / "datasets" / dataset.NAME / size / "dump"
            dump_dir.mkdir(exist_ok=True)
            dir_name = dataset.URL_CSV[size].split("/")[-1:][0].removesuffix(".tar.zst")
            if (dump_dir / dir_name).exists():
                print("Files downloaded")
                dump_dir = dump_dir / dir_name
            else:
                print("Downloading files")
                downloaded_file = helpers.download_file(dataset.URL_CSV[size], dump_dir.absolute())
                print("Unpacking the file..." + downloaded_file)
                dump_dir = helpers.unpack_tar_zst(Path(downloaded_file))

            input_files = {}
            for file in dump_dir.glob("*/*.csv"):
                parts = file.parts[-2:]
                key = parts[0] + "/" + parts[1][:-8]
                input_files[key] = file

            output_files = {}
            for key, file in input_files.items():
                output = file.parent / (file.stem + "_neo" + ".csv")
                if not output.exists():
                    with file.open("r") as input_f, output.open("a") as output_f:
                        header = next(input_f).strip().split("|")
                        reader = csv.reader(input_f, delimiter="|")
                        writer = csv.writer(output_f, delimiter="|")
                        if key in HEADERS_INTERACTIVE.keys():
                            updated_header = HEADERS_INTERACTIVE[key]
                            writer.writerow(updated_header)
                        for line in reader:
                            writer.writerow(line)

                else:
                    output.unlink()

                    # output_files[key] = out

            # subprocess.run(
            #     args=[
            #         vendor._neo4j_admin,
            #         "database",
            #         "import",
            #         "full",
            #         "--nodes=",
            #         nodes,
            #         "--relationships=",
            #         relationships,
            #         "neo4j",
            #     ],
            #     check=True,
            # )

            return True

        elif dataset.NAME == "ldbc_interactive" and isinstance(vendor, Memgraph):
            print("Running Memgraph import")
            return False

        else:
            return False

    def interactive_neo4j():
        pass

    def interactive_memgraph():
        pass

    def bi_neo4j():
        pass

    def bi_memgraph():
        pass
