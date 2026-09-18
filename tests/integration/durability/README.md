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

# TTL in the fixtures

A `ttl` property is a **microsecond** timestamp, the unit the sweeper compares against. The existing
fixtures were authored with `4102444800` — year 2100 read as seconds, but 1970 read as microseconds —
so their TTL entities are permanently expired. New datasets should use `4102444800000000` instead.

Both `record_durability.py` and `runner.py` pass `--storage-ttl-enabled=false` so the sweeper can
never fire inside the second a fixture is being recorded or recovered. The TTL *configuration* is
still recovered and dumped, so `ENABLE TTL ...` remains part of what the expected dumps assert.
