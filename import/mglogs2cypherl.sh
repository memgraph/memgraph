#!/bin/bash -e
COLOR_ORANGE="\e[38;5;208m"
COLOR_GREEN="\e[38;5;35m"
COLOR_RED="\e[0;31m"
COLOR_NULL="\e[0m"

print_help() {
    echo -e "${COLOR_ORANGE}HOW TO RUN:${COLOR_NULL} $0 memgraph_logs_file_path cypherl_output_path"
    exit 1
}

if [ "$#" -ne 2 ]; then
    print_help
fi
INPUT="$1"
OUTPUT="$2"
if [ ! -f "$INPUT" ]; then
    echo -e "${COLOR_RED}ERROR:${COLOR_NULL} memgraph_logs_file_path is not a file!"
    print_help
fi

awk -v RS="Run] '" 'NR>1 { print $0 }' < "$INPUT" | sed -e "/^\[/d;" -e "s/'\([^']*\)$/;/g" > "$OUTPUT"

echo -e "${COLOR_GREEN}DONE!${COLOR_NULL} Please find Memgraph compatible cypherl file under $OUTPUT"
echo ""
echo "Import can be done by executing => \`cat $OUTPUT | mgconsole\`"
