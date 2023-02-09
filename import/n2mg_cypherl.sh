#!/bin/bash -e

# TODO(gitbuda): Test with mgconsole
# TODO(gitbuda): Add help and colors!

INPUT="$1"
OUTPUT="$2"

echo ""
echo "NOTE: BEGIN and COMMIT are required because variables share the same name (e.g. row)"
echo "NOTE: CONSTRAINTS are just skipped -> please create them manually if needed"
echo ""

sed -e 's/^:begin/BEGIN/g; s/^:commit/COMMIT/g;' \
    -e '/^CALL/d; /^SCHEMA AWAIT/d;' \
    -e 's/CREATE RANGE INDEX FOR (n:/CREATE INDEX ON :/g;' \
    -e 's/) ON (n./(/g;' \
    -e '/^CREATE CONSTRAINT/d;' "$INPUT" > "$OUTPUT"

echo ""
echo "DONE! Please find Memgraph compatible cypherl|.cypher file under $OUTPUT"
echo ""
echo "Please import data by executing \`cat $OUTPUT | mgconsole\`"
