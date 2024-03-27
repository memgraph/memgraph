#!/bin/bash -e
COLOR_ORANGE="\e[38;5;208m"
COLOR_GREEN="\e[38;5;35m"
COLOR_RED="\e[0;31m"
COLOR_NULL="\e[0m"

print_help() {
    echo -e "${COLOR_ORANGE}HOW TO RUN:${COLOR_NULL} $0 input_file_schema_path input_file_nodes_path input_file_relationships_path input_file_cleanup_path output_file_schema_path output_file_nodes_path output_file_relationships_path output_file_cleanup_path"
    exit 1
}

if [ "$#" -ne 8 ]; then
    print_help
fi
INPUT_SCHEMA="$1"
INPUT_NODES="$2"
INPUT_RELATIONSHIPS="$3"
INPUT_CLEANUP="$4"
OUTPUT_SCHEMA="$5"
OUTPUT_NODES="$6"
OUTPUT_RELATIONSHIPS="$7"
OUTPUT_CLEANUP="$8"

if [ ! -f "$INPUT_SCHEMA" ]; then
    echo -e "${COLOR_RED}ERROR:${COLOR_NULL} input_file_path is not a file!"
    print_help
fi

if [ ! -f "$INPUT_NODES" ]; then
    echo -e "${COLOR_RED}ERROR:${COLOR_NULL} input_file_path is not a file!"
    print_help
fi

if [ ! -f "$INPUT_RELATIONSHIPS" ]; then
    echo -e "${COLOR_RED}ERROR:${COLOR_NULL} input_file_path is not a file!"
    print_help
fi

if [ ! -f "$INPUT_CLEANUP" ]; then
    echo -e "${COLOR_RED}ERROR:${COLOR_NULL} input_file_path is not a file!"
    print_help
fi

echo -e "${COLOR_ORANGE}NOTE:${COLOR_NULL} BEGIN and COMMIT are required because variables share the same name (e.g. row)"
echo -e "${COLOR_ORANGE}NOTE:${COLOR_NULL} CONSTRAINTS are just skipped -> ${COLOR_RED}please create constraints manually if needed${COLOR_NULL}"


echo 'CREATE INDEX ON :`UNIQUE IMPORT LABEL`(`UNIQUE IMPORT ID`);' > "$OUTPUT_SCHEMA"

sed -e 's/CREATE RANGE INDEX FOR (n:/CREATE INDEX ON :/g;' \
    -e 's/) ON (n./(/g;' \
    -e '/^CREATE CONSTRAINT/d' $INPUT_SCHEMA >> "$OUTPUT_SCHEMA"

cat "$INPUT_NODES" > "$OUTPUT_NODES"
cat "$INPUT_RELATIONSHIPS" > "$OUTPUT_RELATIONSHIPS"

sed -e '/^DROP CONSTRAINT/d' "$INPUT_CLEANUP" >> "$OUTPUT_CLEANUP"

echo 'DROP INDEX ON :`UNIQUE IMPORT LABEL`(`UNIQUE IMPORT ID`);' >> "$OUTPUT_CLEANUP"

echo ""
echo -e "${COLOR_GREEN}DONE!${COLOR_NULL} Please find Memgraph compatible cypherl|.cypher files under $OUTPUT_SCHEMA, $OUTPUT_NODES, $OUTPUT_RELATIONSHIPS and $OUTPUT_CLEANUP"
echo ""
echo "Please import data by executing => \`cat $OUTPUT_SCHEMA | mgconsole\`, \`cat $OUTPUT_NODES | mgconsole\`, \`cat $OUTPUT_RELATIONSHIPS | mgconsole\` and \`cat $OUTPUT_CLEANUP | mgconsole\`"
