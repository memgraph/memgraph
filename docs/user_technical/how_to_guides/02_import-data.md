## How to Import Data?

Memgraph comes with tools for importing data into the database. Currently,
only import of CSV formatted is supported. We plan to support more formats in
the future.

### CSV Import Tool

CSV data should be in Neo4j CSV compatible format. Detailed format
specification can be found
[here](https://neo4j.com/docs/operations-manual/current/tools/import/file-header-format/).

The import tool is run from the console, using the `mg_import_csv` command.

If you installed Memgraph using Docker, you will need to run the importer
using the following command:

```bash
docker run -v mg_lib:/var/lib/memgraph -v mg_etc:/etc/memgraph -v mg_import:/import-data \
  --entrypoint=mg_import_csv memgraph
```

You can pass CSV files containing node data using the `--nodes` option.
Multiple files can be specified by repeating the `--nodes` option.  At least
one node file should be specified. Similarly, graph edges (also known as
relationships) are passed via the `--relationships` option.  Multiple
relationship files are imported by repeating the option. Unlike nodes,
relationships are not required.

After reading the CSV files, the tool will by default search for the installed
Memgraph configuration. If the configuration is found, the data will be
written in the configured durability directory. If the configuration isn't
found, you will need to use the `--out` option to specify the output file. You
can use the same option to override the default behaviour.

Memgraph will recover the imported data on the next startup by looking in the
durability directory.

For information on other options, run:

```bash
mg_import_csv --help
```

When using Docker, this translates to:

```bash
docker run --entrypoint=mg_import_csv memgraph --help
```

#### Example

Let's import a simple dataset.

Store the following in `comment_nodes.csv`.

```csv
id:ID(COMMENT_ID),country:string,browser:string,content:string,:LABEL
0,Croatia,Chrome,yes,Message;Comment
1,United Kingdom,Chrome,thanks,Message;Comment
2,Germany,,LOL,Message;Comment
3,France,Firefox,I see,Message;Comment
4,Italy,Internet Explorer,fine,Message;Comment
```

Now, let's add `forum_nodes.csv`.

```csv
id:ID(FORUM_ID),title:string,:LABEL
0,General,Forum
1,Support,Forum
2,Music,Forum
3,Film,Forum
4,Programming,Forum
```

And finally, set relationships between comments and forums in
`relationships.csv`.

```csv
:START_ID(COMMENT_ID),:END_ID(FORUM_ID),:TYPE
0,0,POSTED_ON
1,1,POSTED_ON
2,2,POSTED_ON
3,3,POSTED_ON
4,4,POSTED_ON
```

Now, you can import the dataset in Memgraph.

WARNING: Your existing recovery data will be considered obsolete, and Memgraph
will load the new dataset.

Use the following command:

```bash
mg_import_csv --overwrite --nodes=comment_nodes.csv --nodes=forum_nodes.csv --relationships=relationships.csv
```

If using Docker, things are a bit more complicated. First you need to move the
CSV files where the Docker image can see them:

```bash
mkdir -p /var/lib/docker/volumes/mg_import/_data
cp comment_nodes.csv forum_nodes.csv relationships.csv /var/lib/docker/volumes/mg_import/_data
```

Then, run the importer with the following:

```bash
docker run -v mg_lib:/var/lib/memgraph -v mg_etc:/etc/memgraph -v mg_import:/import-data \
  --entrypoint=mg_import_csv memgraph \
  --overwrite \
  --nodes=/import-data/comment_nodes.csv --nodes=/import-data/forum_nodes.csv \
  --relationships=/import-data/relationships.csv
```

Next time you run Memgraph, the dataset will be loaded.
