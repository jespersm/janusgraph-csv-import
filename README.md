# JanusGraph CSV Loader

JanusGraph CSV Loader is a Java utility for bulk-loading data into a JanusGraph database.

## Usage

```
java com.github.jespersm.janusgraph.csvimport.Import [-D] [--add-label-property] [--ignore-missing-nodes]
                    [--threads=<poolSize>] -c=<configFile> [-n=<limitRows>]
                    [--edgeLabels=<edgeLabels>[,<edgeLabels>...]]...
                    --nodes=<label=file1,file2>,<label=file1,file2>...
                    [--relationships=<file1,file2>]...
```
Where options are:
```
  -D, --drop-before-import                 Drop the graph before importing it
      --add-label-property                 Add a _label property to each Vertex, Edge copying the "real" label.
      --edgeLabels=<edgeLabels> ...        Import a CSV file with edge definitions
      --ignore-missing-nodes               Skip edges which hasn't had it's IDs imported.
      --nodes=<label=file1,file2,...>      Import vertices from file1 etc, using the given label name, and the
                                           headers (from the first file)
      --relationships=<file1,file2,...>    Import edges/relationships from file1, etc.

      --threads=<poolSize>                 Number of threads to run concurrently when importing vertixes/edges
  -c, --config=<configFile>                Identify the config file for creating JanusGraphFactory
  -n, --limit-rows=<limitRows>             Only import this many vertices/edges per type, useful for testing

```

## CSV file format
The CSV files have a header line, containing the declaration of each column. Each declaration has a column name, an optional type and an optional column "tag", separated by colon. Example: `fleep:uuid:ID`

_Names_ correspond to property key name. If blank, no property is created.

The _type_ specifies how the JanusGraph schema is initialized: 
* `string`
* `int`
* `long`
* `float`
* `double`
* `boolean`
* `byte`
* `short`
* `char`
* `datetime` (maps to java.util.Date)
* `uuid` (maps to binary UUID).

The following are also recognized, but mapped to string, since JanusGraph doesn't support JSR-310 date/time types natively:
* `date`
* `localtime`
* `time`
* `localdatetime`
* `duration`

The _tag_ is how the value is used:
* ID - recognize this column as the vertex ID (not supported for edges). Also generates a unique index.
* DATA - just store this column (the default)
* INDEX - index this property (limited to containing vertex label)
* UNIQUE - make unique index for this property (under the containing vertex label)
* IGNORE - don't import this column

For files containing information about edges only:
* START_ID - specifies the ID of the starting vertex ("out")
* END_ID - specifies the ID of the ending vertex
* TYPE - The label of the edge

Remember, the types of the IDs and START_ID / END_ID must match. There is no warning against that.

## Examples:

Assume you have this setting file, `import.properties`:
```
storage.backend=berkeleyje
storage.directory=some-folder
```
(please see [JanusGraph configuration documentation](https://docs.janusgraph.org/0.3.1/configuration.html) for what to put into this file)

And you have this file containing persons, `nodes.csv`:
```
id:long:ID,name:string
1,"Bob"
2,"Alice"
3,"Charlie"
```

And you have this file containing relationships, `edges.csv`:
```
id:long:START_ID,id:long:END_ID,:TYPE,since:date
1,2,"KNOWS",2018-06-24
2,3,"MET",2014-03-11
```

Now, you can import the graph by using Gradle directly:

```
$ ./gradlew run --args="--config=import.properties --nodes=Person=nodes.csv --relationships=edges.csv"
```

This will populate the tree nodes and two edges into a JanusGraph graph database backed by BerkeleyJE, as specified in the config file.

You can use the "shadowJar" task in Gradle to build a fat Jar containing all the dependencies for running the importer without Gradle.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[APLv2](https://www.apache.org/licenses/LICENSE-2.0)
