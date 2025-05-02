#!/bin/bash

# Run the Python script in a loop until it fails
while python3 runner.py --workloads-root-directory . --workload-name "Read-write benchmark"; do
  echo "Run succeeded, repeating..."
done

echo "Run failed. Exiting loop."

