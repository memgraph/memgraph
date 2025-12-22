# To make a new trigger recording

1. Make a new version under the `tests` folder
   - e.g. `tests/v3`
2. Create `create_dataset.cypher` that creates all triggers
   - Make sure it captures all important aspects from previous versions
   - And also captures the extra trigger case which cause the version to change
   - Include triggers with different privilege contexts (SECURITY INVOKER, SECURITY DEFINER, and default)
3. Run Memgraph with the create_dataset.cypher to generate the triggers directory
4. Copy the entire triggers directory from `.durability/memgraph/triggers` to `tests/v3/triggers`
5. Run `./runner.py --write-expected` to capture the expected dump output

# To verify

1. Run `./runner.py`

The test verifies:
- Triggers can be restored from stored trigger directory
- `DUMP DATABASE` includes triggers correctly and matches expected dump output
