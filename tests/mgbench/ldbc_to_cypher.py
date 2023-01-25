import argparse
import csv
from datetime import datetime
from pathlib import Path

# Full LDBC datasets available at: https://github.com/ldbc/data-sets-surf-repository

NODES = [
    {"filename": "City", "label": "City"},
    {"filename": "Comment", "label": "Comment"},
    {"filename": "Company", "label": "Company"},
    {"filename": "Continent", "label": "Continent"},
    {"filename": "Country", "label": "Country"},
    {"filename": "Forum", "label": "Forum"},
    {"filename": "Person", "label": "Person"},
    {"filename": "Post", "label": "Post"},
    {"filename": "TagClass", "label": "TagClass"},
    {"filename": "Tag", "label": "Tag"},
    {"filename": "University", "label": "University"},
]

EDGES = [
    {"filename": "City_isPartOf_Country", "source_label": "City", "type": "IS_PART_OF", "target_label": "Country"},
    {
        "filename": "Comment_hasCreator_Person",
        "source_label": "Comment",
        "type": "HAS_CREATOR",
        "target_label": "Person",
    },
    {"filename": "Comment_hasTag_Tag", "source_label": "Comment", "type": "HAS_TAG", "target_label": "Tag"},
    {
        "filename": "Comment_isLocatedIn_Country",
        "source_label": "Comment",
        "type": "IS_LOCATED_IN",
        "target_label": "Country",
    },
    {"filename": "Comment_replyOf_Comment", "source_label": "Comment", "type": "REPLY_OF", "target_label": "Comment"},
    {"filename": "Comment_replyOf_Post", "source_label": "Comment", "type": "REPLY_OF", "target_label": "Post"},
    {
        "filename": "Company_isLocatedIn_Country",
        "source_label": "Company",
        "type": "IS_LOCATED_IN",
        "target_label": "Country",
    },
    {
        "filename": "Country_isPartOf_Continent",
        "source_label": "Country",
        "type": "IS_PART_OF",
        "target_label": "Continent",
    },
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
    {"filename": "Person_isLocatedIn_City", "source_label": "Person", "type": "IS_LOCATED_IN", "target_label": "City"},
    {"filename": "Person_knows_Person", "source_label": "Person", "type": "KNOWS", "target_label": "Person"},
    {"filename": "Person_likes_Comment", "source_label": "Person", "type": "LIKES", "target_label": "Comment"},
    {"filename": "Person_likes_Post", "source_label": "Person", "type": "LIKES", "target_label": "Post"},
    {
        "filename": "Person_studyAt_University",
        "source_label": "Person",
        "type": "STUDY_AT",
        "target_label": "University",
    },
    {"filename": "Person_workAt_Company", "source_label": "Person", "type": "WORK_AT", "target_label": "Company"},
    {"filename": "Post_hasCreator_Person", "source_label": "Post", "type": "HAS_CREATOR", "target_label": "Person"},
    {"filename": "Post_hasTag_Tag", "source_label": "Post", "type": "HAS_TAG", "target_label": "Tag"},
    {
        "filename": "Post_isLocatedIn_Country",
        "source_label": "Post",
        "type": "IS_LOCATED_IN",
        "target_label": "Country",
    },
    {
        "filename": "TagClass_isSubclassOf_TagClass",
        "source_label": "TagClass",
        "type": "IS_SUBCLASS_OF",
        "target_label": "TagClass",
    },
    {"filename": "Tag_hasType_TagClass", "source_label": "Tag", "type": "HAS_TYPE", "target_label": "TagClass"},
    {
        "filename": "University_isLocatedIn_City",
        "source_label": "University",
        "type": "IS_LOCATED_IN",
        "target_label": "City",
    },
]

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="LDBC CSV to CYPHERL converter",
        description="""Converts all LDBC CSV files to CYPHERL transactions, each CSV file maps to proper CYPHERL transacion file.
                                    Each CSV is read without header beacuse of hader naming scheme. CSV files need to be uncompressd from tar.zst format""",
    )

    parser.add_argument("--ldbc", type=Path, required=True, help="Path to parent CSV directory.")
    parser.add_argument(
        "--output", type=Path, required=True, help="Path to output directory, will be created if does not exist."
    )
    parser.add_argument(
        "--single-cypherl", action="store_true", help="Argument for saving everything in single cypher file. "
    )

    args = parser.parse_args()
    args.output.mkdir(exist_ok=True)
    out = "out.cypher"
    out_file = args.output / out
    if out_file.exists():
        out_file.unlink()

    input_files = {}
    for file in args.ldbc.glob("**/*.csv"):
        name = file.name.replace("_0_0.csv", "").lower()
        input_files[name] = file

    if args.single_cypherl:
        for node_file in NODES:
            key = node_file["filename"].lower()
            label = node_file["label"]
            query = None
            if key in input_files.keys():
                with input_files[key].open("r") as input_f, out_file.open("a") as output_f:
                    reader = csv.DictReader(input_f, delimiter="|")
                    for row in reader:
                        query = "CREATE (:{} {{id:{}, ".format(label, row.pop("id"))
                        # Format properties
                        for k, v in row.items():
                            if k == "creationDate":
                                # Take time zone out
                                row[k] = 'localDateTime("{}")'.format(v[0:-5])
                            else:
                                row[k] = '"{}"'.format(v)

                        prop_string = ", ".join("{} : {}".format(k, v) for k, v in row.items())
                        query = query + prop_string + "});"
                        output_f.write(query + "\n")
                print("Line")
                print("Converted file: " + input_files[key].name + " to " + out_file.name)

        for edge_file in EDGES:
            key = edge_file["filename"].lower()
            source_label = edge_file["source_label"]
            edge_type = edge_file["type"]
            target_label = edge_file["target_label"]
            if key in input_files.keys():
                out_file = args.output / out
                query = None
                with input_files[key].open("r") as input_f, out_file.open("a") as output_f:
                    sufixl = ".id"
                    sufixr = ".id"
                    # Handle identical label/key in CSV header
                    if source_label == target_label:
                        sufixl = "l"
                        sufixr = "r"
                    # Move a place from header
                    next(input_f)
                    reader = csv.DictReader(
                        input_f, delimiter="|", fieldnames=[source_label + sufixl, target_label + sufixr]
                    )

                    for row in reader:
                        query = "MATCH (n1:{} {{id:{}}}), (n2:{} {{id:{}}}) ".format(
                            source_label, row.pop(source_label + sufixl), target_label, row.pop(target_label + sufixr)
                        )
                        # TODO(antejavor): Add support for edges properties.
                        # prop_string = ", ".join("{} : \"{}\"".format(k, v) for k, v in row.items())
                        edge_part = "CREATE (n1)-[:{}]->(n2);".format(edge_type)
                        query = query + edge_part
                        output_f.write(query + "\n")
                print("line")
                print("Converted file: " + input_files[key].name + " to " + out_file.name)
    else:
        # TODO(antejavor): Add support for multiple files.
        pass
