import argparse
import csv
import os
from collections import defaultdict
from pathlib import Path

import helpers

# Full LDBC datasets available at: https://github.com/ldbc/data-sets-surf-repository

NODES_INTERACTIVE = [
    {"filename": "Place", "label": "Place"},
    {"filename": "Organisation", "label": "Organisation"},
    {"filename": "TagClass", "label": "TagClass"},
    {"filename": "Tag", "label": "Tag"},
    {"filename": "Comment", "label": "Message:Comment"},
    {"filename": "Forum", "label": "Forum"},
    {"filename": "Person", "label": "Person"},
    {"filename": "Post", "label": "Message:Post"},
]

EDGES_INTERACTIVE = [
    {
        "filename": "Place_isPartOf_Place",
        "source_label": "Place",
        "type": "IS_PART_OF",
        "target_label": "Place",
    },
    {
        "filename": "TagClass_isSubclassOf_TagClass",
        "source_label": "TagClass",
        "type": "IS_SUBCLASS_OF",
        "target_label": "TagClass",
    },
    {
        "filename": "Organisation_isLocatedIn_Place",
        "source_label": "Organisation",
        "type": "IS_LOCATED_IN",
        "target_label": "Place",
    },
    {"filename": "Tag_hasType_TagClass", "source_label": "Tag", "type": "HAS_TYPE", "target_label": "TagClass"},
    {
        "filename": "Comment_hasCreator_Person",
        "source_label": "Comment",
        "type": "HAS_CREATOR",
        "target_label": "Person",
    },
    {
        "filename": "Comment_isLocatedIn_Place",
        "source_label": "Comment",
        "type": "IS_LOCATED_IN",
        "target_label": "Place",
    },
    {"filename": "Comment_replyOf_Comment", "source_label": "Comment", "type": "REPLY_OF", "target_label": "Comment"},
    {"filename": "Comment_replyOf_Post", "source_label": "Comment", "type": "REPLY_OF", "target_label": "Post"},
    {"filename": "Forum_containerOf_Post", "source_label": "Forum", "type": "CONTAINER_OF", "target_label": "Post"},
    {"filename": "Forum_hasMember_Person", "source_label": "Forum", "type": "HAS_MEMBER", "target_label": "Person"},
    {
        "filename": "Forum_hasModerator_Person",
        "source_label": "Forum",
        "type": "HAS_MODERATOR",
        "target_label": "Person",
    },
    {"filename": "Forum_hasTag_Tag", "source_label": "Forum", "type": "HAS_TAG", "target_label": "Tag"},
    {"filename": "Person_hasInterest_Tag", "source_label": "Person", "type": "HAS_INTEREST", "target_label": "Tag"},
    {
        "filename": "Person_isLocatedIn_Place",
        "source_label": "Person",
        "type": "IS_LOCATED_IN",
        "target_label": "Place",
    },
    {"filename": "Person_knows_Person", "source_label": "Person", "type": "KNOWS", "target_label": "Person"},
    {"filename": "Person_likes_Comment", "source_label": "Person", "type": "LIKES", "target_label": "Comment"},
    {"filename": "Person_likes_Post", "source_label": "Person", "type": "LIKES", "target_label": "Post"},
    {"filename": "Post_hasCreator_Person", "source_label": "Post", "type": "HAS_CREATOR", "target_label": "Person"},
    {"filename": "Comment_hasTag_Tag", "source_label": "Comment", "type": "HAS_TAG", "target_label": "Tag"},
    {"filename": "Post_hasTag_Tag", "source_label": "Post", "type": "HAS_TAG", "target_label": "Tag"},
    {
        "filename": "Post_isLocatedIn_Place",
        "source_label": "Post",
        "type": "IS_LOCATED_IN",
        "target_label": "Place",
    },
    {
        "filename": "Person_studyAt_Organisation",
        "source_label": "Person",
        "type": "STUDY_AT",
        "target_label": "Organisation",
    },
    {
        "filename": "Person_workAt_Organisation",
        "source_label": "Person",
        "type": "WORK_AT",
        "target_label": "Organisation",
    },
]

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="LDBC CSV to CYPHERL converter",
        description="""Converts all LDBC CSV files to CYPHERL transactions, each CSV file maps to proper CYPHERL transaction file.
                                    Each CSV is read without header because of header naming scheme. CSV files need to be uncompressed from tar.zst format""",
    )
    # parser.add_argument("--size", help="sf1, sf3, sf10, sf30")

    parser.add_argument("--ldbc", type=Path, required=True, help="Path to parent CSV directory.")
    parser.add_argument(
        "--output", type=Path, required=True, help="Path to output directory, will be created if does not exist."
    )
    parser.add_argument("--type", required=True, help="interactive or bi")

    args = parser.parse_args()
    args.output.mkdir(exist_ok=True)

    if args.type == "interactive":
        out = "ldbc_interactive_sf10.cypher"
        out_file = args.output / out
        if out_file.exists():
            out_file.unlink()

        input_files = {}
        for file in args.ldbc.glob("**/*.csv"):
            name = file.name.replace("_0_0.csv", "").lower()
            input_files[name] = file

        for node_file in NODES_INTERACTIVE:
            key = node_file["filename"].lower()
            default_label = node_file["label"]
            query = None
            if key in input_files.keys():
                with input_files[key].open("r") as input_f, out_file.open("a") as output_f:
                    reader = csv.DictReader(input_f, delimiter="|")

                    for row in reader:
                        if "type" in row.keys():
                            label = default_label + ":" + row.pop("type").capitalize()
                        else:
                            label = default_label

                        query = "CREATE (:{} {{id:{}, ".format(label, row.pop("id"))
                        # Format properties to fit Memgraph
                        for k, v in row.items():
                            if k == "creationDate":
                                row[k] = 'localDateTime("{}")'.format(v[0:-5])
                            elif k == "birthday":
                                row[k] = 'date("{}")'.format(v)
                            elif k == "length":
                                row[k] = "toInteger({})".format(v)
                            else:
                                row[k] = '"{}"'.format(v)

                        prop_string = ", ".join("{} : {}".format(k, v) for k, v in row.items())
                        query = query + prop_string + "});"
                        output_f.write(query + "\n")
                print("Converted file: " + input_files[key].name + " to " + out_file.name)
            else:
                print("Didn't process node file: " + key)
                raise Exception("Didn't find the file that was needed!")

        for edge_file in EDGES_INTERACTIVE:
            key = edge_file["filename"].lower()
            source_label = edge_file["source_label"]
            edge_type = edge_file["type"]
            target_label = edge_file["target_label"]
            if key in input_files.keys():
                query = None
                with input_files[key].open("r") as input_f, out_file.open("a") as output_f:
                    sufixl = ".id"
                    sufixr = ".id"
                    # Handle identical label/key in CSV header
                    if source_label == target_label:
                        sufixl = "l"
                        sufixr = "r"
                    # Move a place from header
                    header = next(input_f).strip().split("|")
                    reader = csv.DictReader(
                        input_f, delimiter="|", fieldnames=([source_label + sufixl, target_label + sufixr] + header[2:])
                    )

                    for row in reader:
                        query = "MATCH (n1:{} {{id:{}}}), (n2:{} {{id:{}}}) ".format(
                            source_label, row.pop(source_label + sufixl), target_label, row.pop(target_label + sufixr)
                        )
                        for k, v in row.items():
                            if "date" in k.lower():
                                # Take time zone out
                                row[k] = 'localDateTime("{}")'.format(v[0:-5])
                            else:
                                row[k] = '"{}"'.format(v)

                        edge_part = "CREATE (n1)-[:{}{{".format(edge_type)
                        prop_string = ", ".join("{} : {}".format(k, v) for k, v in row.items())

                        query = query + edge_part + prop_string + "}]->(n2);"
                        output_f.write(query + "\n")
                print("Converted file: " + input_files[key].name + " to " + out_file.name)
            else:
                print("Didn't process Edge file: " + key)
                raise Exception("Didn't find the file that was needed!")

    elif args.type == "bi":

        NODES_BI = [
            {"filename": "Place", "label": "Place"},
            {"filename": "Organisation", "label": "Organisation"},
            {"filename": "TagClass", "label": "TagClass"},
            {"filename": "Tag", "label": "Tag"},
            {"filename": "Comment", "label": "Message:Comment"},
            {"filename": "Forum", "label": "Forum"},
            {"filename": "Person", "label": "Person"},
            {"filename": "Post", "label": "Message:Post"},
        ]

        EDGES_BI = [
            {
                "filename": "Place_isPartOf_Place",
                "source_label": "Place",
                "type": "IS_PART_OF",
                "target_label": "Place",
            },
            {
                "filename": "TagClass_isSubclassOf_TagClass",
                "source_label": "TagClass",
                "type": "IS_SUBCLASS_OF",
                "target_label": "TagClass",
            },
            {
                "filename": "Organisation_isLocatedIn_Place",
                "source_label": "Organisation",
                "type": "IS_LOCATED_IN",
                "target_label": "Place",
            },
            {"filename": "Tag_hasType_TagClass", "source_label": "Tag", "type": "HAS_TYPE", "target_label": "TagClass"},
            {
                "filename": "Comment_hasCreator_Person",
                "source_label": "Comment",
                "type": "HAS_CREATOR",
                "target_label": "Person",
            },
            # Change place to Country
            {
                "filename": "Comment_isLocatedIn_Country",
                "source_label": "Comment",
                "type": "IS_LOCATED_IN",
                "target_label": "Country",
            },
            {
                "filename": "Comment_replyOf_Comment",
                "source_label": "Comment",
                "type": "REPLY_OF",
                "target_label": "Comment",
            },
            {"filename": "Comment_replyOf_Post", "source_label": "Comment", "type": "REPLY_OF", "target_label": "Post"},
            {
                "filename": "Forum_containerOf_Post",
                "source_label": "Forum",
                "type": "CONTAINER_OF",
                "target_label": "Post",
            },
            {
                "filename": "Forum_hasMember_Person",
                "source_label": "Forum",
                "type": "HAS_MEMBER",
                "target_label": "Person",
            },
            {
                "filename": "Forum_hasModerator_Person",
                "source_label": "Forum",
                "type": "HAS_MODERATOR",
                "target_label": "Person",
            },
            {"filename": "Forum_hasTag_Tag", "source_label": "Forum", "type": "HAS_TAG", "target_label": "Tag"},
            {
                "filename": "Person_hasInterest_Tag",
                "source_label": "Person",
                "type": "HAS_INTEREST",
                "target_label": "Tag",
            },
            # Changed place to City
            {
                "filename": "Person_isLocatedIn_City",
                "source_label": "Person",
                "type": "IS_LOCATED_IN",
                "target_label": "City",
            },
            {"filename": "Person_knows_Person", "source_label": "Person", "type": "KNOWS", "target_label": "Person"},
            {"filename": "Person_likes_Comment", "source_label": "Person", "type": "LIKES", "target_label": "Comment"},
            {"filename": "Person_likes_Post", "source_label": "Person", "type": "LIKES", "target_label": "Post"},
            {
                "filename": "Post_hasCreator_Person",
                "source_label": "Post",
                "type": "HAS_CREATOR",
                "target_label": "Person",
            },
            {"filename": "Comment_hasTag_Tag", "source_label": "Comment", "type": "HAS_TAG", "target_label": "Tag"},
            {"filename": "Post_hasTag_Tag", "source_label": "Post", "type": "HAS_TAG", "target_label": "Tag"},
            # Change place to Country
            {
                "filename": "Post_isLocatedIn_Country",
                "source_label": "Post",
                "type": "IS_LOCATED_IN",
                "target_label": "Country",
            },
            # Changed organisation to University
            {
                "filename": "Person_studyAt_University",
                "source_label": "Person",
                "type": "STUDY_AT",
                "target_label": "University",
            },
            # Changed organisation to Company
            {
                "filename": "Person_workAt_Company",
                "source_label": "Person",
                "type": "WORK_AT",
                "target_label": "Company",
            },
        ]

        out = "test.cypher"
        out_file = args.output / out
        if out_file.exists():
            out_file.unlink()

        for file in args.ldbc.glob("**/*.csv.gz"):
            if "initial_snapshot" in file.parts:
                helpers.unpack(file)

        input_files = defaultdict(list)
        for file in args.ldbc.glob("**/*.csv"):
            key = file.parents[0].name
            input_files[file.parents[0].name].append(file)

        for node_file in NODES_BI:
            key = node_file["filename"]
            default_label = node_file["label"]
            query = None
            if key in input_files.keys():
                for part_file in input_files[key]:
                    with part_file.open("r") as input_f, out_file.open("a") as output_f:
                        reader = csv.DictReader(input_f, delimiter="|")

                        for row in reader:
                            if "type" in row.keys():
                                label = default_label + ":" + row.pop("type")
                            else:
                                label = default_label

                            query = "CREATE (:{} {{id:{}, ".format(label, row.pop("id"))
                            # Format properties to fit Memgraph
                            for k, v in row.items():
                                if k == "creationDate":
                                    row[k] = 'localDateTime("{}")'.format(v[0:-6])
                                elif k == "birthday":
                                    row[k] = 'date("{}")'.format(v)
                                elif k == "length":
                                    row[k] = "toInteger({})".format(v)
                                else:
                                    row[k] = '"{}"'.format(v)

                            prop_string = ", ".join("{} : {}".format(k, v) for k, v in row.items())
                            query = query + prop_string + "});"
                            output_f.write(query + "\n")
                    print("Key: " + key + " Converted file: " + part_file.name + " to " + out_file.name)
            else:
                print("Didn't process node file: " + key)

        for edge_file in EDGES_BI:
            key = edge_file["filename"]
            source_label = edge_file["source_label"]
            edge_type = edge_file["type"]
            target_label = edge_file["target_label"]
            if key in input_files.keys():
                for part_file in input_files[key]:
                    query = None
                    with part_file.open("r") as input_f, out_file.open("a") as output_f:
                        sufixl = "Id"
                        sufixr = "Id"
                        # Handle identical label/key in CSV header
                        if source_label == target_label:
                            sufixl = "l"
                            sufixr = "r"
                        # Move a place from header
                        header = next(input_f).strip().split("|")
                        if len(header) >= 3:
                            reader = csv.DictReader(
                                input_f,
                                delimiter="|",
                                fieldnames=(["date", source_label + sufixl, target_label + sufixr] + header[3:]),
                            )
                        else:
                            reader = csv.DictReader(
                                input_f,
                                delimiter="|",
                                fieldnames=([source_label + sufixl, target_label + sufixr] + header[2:]),
                            )

                        for row in reader:
                            query = "MATCH (n1:{} {{id:{}}}), (n2:{} {{id:{}}}) ".format(
                                source_label,
                                row.pop(source_label + sufixl),
                                target_label,
                                row.pop(target_label + sufixr),
                            )
                            for k, v in row.items():
                                if "date" in k.lower():
                                    # Take time zone out
                                    row[k] = 'localDateTime("{}")'.format(v[0:-6])
                                else:
                                    row[k] = '"{}"'.format(v)

                            edge_part = "CREATE (n1)-[:{}{{".format(edge_type)
                            prop_string = ", ".join("{} : {}".format(k, v) for k, v in row.items())

                            query = query + edge_part + prop_string + "}]->(n2);"
                            output_f.write(query + "\n")
                        print("Line")
                    print("Key: " + key + " Converted file: " + part_file.name + " to " + out_file.name)
            else:
                print("Didn't process Edge file: " + key)
                raise Exception("Didn't find the file that was needed!")
