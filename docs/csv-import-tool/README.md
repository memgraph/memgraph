# CSV Import Tool Documentation

CSV is a universal and very versatile data format used to store large quantities
of data.  Each Memgraph database instance has a CSV import tool installed called
`mg_import_csv`.  The CSV import tool should be used for initial bulk ingestion
of data into the database.  Upon ingestion, the CSV importer creates a snapshot
that will be used by the database to recover its state on its next startup.

If you are already familiar with the Neo4j bulk import tool, then using the
`mg_import_csv` tool should be easy.  The CSV import tool is fully compatible
with the [Neo4j CSV
format](https://neo4j.com/docs/operations-manual/current/tools/import/). If you
already have a pipeline set-up for Neo4j, you should only replace `neo4j-admin
import` with `mg_import_csv`.

## CSV File Format

Each row of a CSV file represents a single entry that should be imported into
the database.  Both nodes and relationships can be imported into the database
using CSV files.

Each set of CSV files must have a header that describes the data that is stored
in the CSV files. Each field in the CSV header is in the format
`<name>[:<type>]` which identifies the name that should be used for that column
and the type that should be used for that column. The type is optional and
defaults to `string` (see the following chapter).

Each CSV field must be divided using the delimiter and each CSV field can either
be quoted or unquoted. When the field is quoted, the first and last character in
the field *must* be the quote character. If the field isn't quoted, and a quote
character appears in it, it is treated as a regular character. If a quote
character appears inside a quoted string then the quote character must be
doubled in order to escape it. Line feeds and carriage returns are ignored in
the CSV file, also, the file can't contain a NULL character.

## Properties

Both nodes and relationships can have properties added to them.  When importing
properties, the CSV importer uses the name specified in the header of the
corresponding CSV column for the name of the property.  A property is designated
by specifying one of the following types in the header:
 - `integer`, `int`, `long`, `byte`, `short`: creates an integer property
 - `float`, `double`: creates a float property
 - `boolean`, `bool`: creates a boolean property
 - `string`, `char`: creates a string property

When importing a boolean value, the CSV field should contain exactly the text
`true` to import a `True` boolean value.  All other text values are treated as a
boolean value `False`.

If you want to import an array of values, you can do so by appending `[]` to any
of the above types.  The values of the array are then determined by splitting
the raw CSV value using the array delimiter character.

Assuming that the array delimiter is `;`, the following example:
```plaintext
first_name,last_name:string,number:integer,aliases:string[]
John,Doe,1,Johnny;Jo;J-man
Melissa,Doe,2,Mel
```

Will yield these results:
```plaintext
CREATE ({first_name: "John", last_name: "Doe", number: 1, aliases: ["Johnny", "Jo", "J-man"]});
CREATE ({first_name: "Melissa", last_name: "Doe", number: 2, aliases: ["Mel"]});
```
### Nodes

When importing nodes, several more types can be specified in the header of the
CSV file (along with all property types):
 - `ID`: id of the node that should be used as the node ID when importing
   relationships
 - `LABEL`: designates that the field contains additional labels for the node
 - `IGNORE`: designates that the field should be ignored

The `ID` field type sets the internal ID that will be used for the node when
creating relationships.  It is optional and nodes that don't have an ID value
specified will be imported, but can't be connected to any relationships.  If you
want to save the ID value as a property in the database, just specify a name for
the ID (`user_id:ID`).  If you just want to use the ID during the import, leave
out the name of the field (`:ID`).  The `ID` field also supports creating
separate ID spaces.  The ID space is specified with the ID space name appended
to the `ID` type in parentheses (`ID(user)`).  That allows you to have the same
IDs (by value) for multiple different node files (for example, numbers from 1 to
N).  The IDs in each ID space will be treated as an independent set of IDs that
don't interfere with IDs in another ID space.

The `LABEL` field type adds additional labels to the node.  The value is treated
as an array type so that multiple additional labels can be specified for each
node.  The value is split using the array delimiter (`--array-delimiter` flag).

### Relationships

In order to be able to import relationships, you must import the nodes in the
same invocation of `mg_import_csv` that is used to import the relationships.

When importing relationships, several more types can be specified in the header
of the CSV file (along with all property types):
 - `START_ID`: id of the start node that should be connected with the
   relationship
 - `END_ID`: id of the end node that should be connected with the relationship
 - `TYPE`: designates the type of the relationship
 - `IGNORE`: designates that the field should be ignored

The `START_ID` field type sets the start node that should be connected with the
relationship to the end node.  The field *must* be specified and the node ID
must be one of the node IDs that were specified in the node CSV files.  The name
of this field is ignored.  If the node ID is in an ID space, you can specify the
ID space for the in the same way as for the node ID (`START_ID(user)`).

The `END_ID` field type sets the end node that should be connected with the
relationship to the start node.  The field *must* be specified and the node ID
must be one of the node IDs that were specified in the node CSV files.  The name
of this field is ignored.  If the node ID is in an ID space, you can specify the
ID space for the in the same way as for the node ID (`END_ID(user)`).

The `TYPE` field type sets the type of the relationship.  Each relationship
*must* have a relationship type, but it doesn't necessarily need to be specified
in the CSV file, it can also be set externally for the whole CSV file.  The name
of this field is ignored.

## CSV Importer Flags

The importer has many command line options that allow you to customize the way
the importer loads your data.

The two main flags that are used to specify the input CSV files are `--nodes`
and `--relationships`. Basic description of these flags is provided in the table
and more detailed explanation can be found further down bellow.


| Flag                  | Description  |
|-----------------------| -------------- |
|`--nodes`                | Used to specify CSV files that contain the nodes to the importer. |
|`--relationships`        |  Used to specify CSV files that contain the relationships to the importer.|
|`--delimiter`            | Sets the delimiter that should be used when splitting the CSV fields (default `,`)|
|`--quote`                | Sets the quote character that should be used to quote a CSV field (default `"`)|
|`--array-delimiter`      | Sets the delimiter that should be used when splitting array values (default `;`)|
|`--id-type`              | Specifies which data type should be used to store the supplied <br /> node IDs when storing them as properties (if the field name is supplied). <br /> The supported values are either `STRING` or `INTEGER`. (default `STRING`)|
|`--ignore-empty-strings` | Instructs the importer to treat all empty strings as `Null` values  <br /> instead of an empty string value (default `false`)|
|`--ignore-extra-columns` | Instructs the importer to ignore all columns (instead of raising an error) <br /> that aren't specified after the last specified column in the CSV header. (default `false`) |
| `--skip-bad-relationships`| Instructs the importer to ignore all relationships (instead of raising an error) <br /> that refer to nodes that don't exist in the node files. (default `false`) |
|`--skip-duplicate-nodes`  | Instructs the importer to ignore all duplicate nodes (instead of raising an error).  <br /> Duplicate nodes are nodes that have an ID that is the same as another node that was already imported. (default `false`) |
| `--trim-strings`| Instructs the importer to trim all of the loaded CSV field values before processing them further. <br /> Trimming the fields removes all leading and trailing whitespace from them. (default `false`) |

The `--nodes` and  `--relationships` flags are used to specify CSV files that
contain the nodes and relationships to the importer.  Multiple files can be
specified in each supplied `--nodes` or `--relationships` flag. Files that are
supplied in one `--nodes` or `--relationships` flag are treated by the CSV
parser as one big CSV file.  Only the first line of the first file is parsed for
the CSV header, all other files (and rows) are treated as data.  This is useful
when you have a very large CSV file and don't want to edit its first line just
to add a CSV header.  Instead, you can specify the header in a separate file
(e.g. `users_header.csv` or `friendships_header.csv`) and have the data intact
in the large file (e.g. `users.csv` or `friendships.csv`).  Also, you can supply
additional labels for each set of node files.

The format of `--nodes` flag is:
`[<label>[:<label>]...=]<file>[,<file>][,<file>]...`.  Take note that only the
first `<file>` part is mandatory, all other parts of the flag value are
optional. Multiple `--nodes` flags can be supplied to describe multiple sets of
different node files.  For the importer to work, at least one `--nodes` flag
*must* be supplied.

The format of `--relationships` flag is: `[<type>=]<file>[,<file>][,<file>]...`.
Take note that only the first `<file>` part is mandatory, all other parts of the
flag value are optional.  Multiple `--relationships` flags can be supplied to
describe multiple sets of different relationship files.  The `--relationships`
flag isn't mandatory.

## CSV Parser Logic

The CSV parser uses the same logic as the standard Python CSV parser.  The data
is parsed in the same way as the following snippet:

```python
import csv
for row in csv.reader(stream, strict=True):
    # process 'row'
```

Python uses 'excel' as the default dialect when parsing CSV files and the
default settings for the CSV parser are:
 - delimiter: `','`
 - doublequote: `True`
 - escapechar: `None`
 - lineterminator: `'\r\n'`
 - quotechar: `'"'`
 - skipinitialspace: `False`

The above snippet can be expanded to:

```python
import csv
for row in csv.reader(stream, delimiter=',', doublequote=True,
                      escapechar=None, lineterminator='\r\n',
                      quotechar='"', skipinitialspace=False,
                      strict=True):
    # process 'row'
```

For more information about the meaning of the above values, see:
https://docs.python.org/3/library/csv.html#csv.Dialect

