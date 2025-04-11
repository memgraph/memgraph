# To make a new durability recording

1. Make a new version under the `tests` folder
   - e.g. `tests/v19`
2. Each subfolder of that must contain `create_dataset.cypher` that be used to make the dataset
   - e.g. `tests/v19/test_all/create_dataset.cypher`
   - make sure it captures all important aspects from previous versions
   - and also catures the extra durability case which cause the version to change
3. run `./record_durability.py` to capture the snapshot and wal binaries
4. run `./runner.py --write-expected` to capture the cypher dumps

# To verify

1. Run `./runner.py`
