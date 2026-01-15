#!/bin/bash
latest=$(curl -s https://api.github.com/repos/memgraph/memgraph/tags | python3 -c "
import sys, json
tags = json.load(sys.stdin)
print(next(tag['name'][1:] for tag in tags if 'rc' not in tag['name']))
")
echo "$latest"
