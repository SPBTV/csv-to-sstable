

# CSV to Cassandra SSTable

Converts a CSV file into SSTables that can be bulkloaded into a Cassandra cluster

## Installation

Make sure that maven is installed on your system. Then to build the jar file, run:

    $ ./build

## Usage

When running csv-to-sstable.jar, you need to specify the keyspace for which you want to generate the SSTables, the absolute path to a CQL file containing the "TABLE CREATE" definition of the Cassandra table that you want to populate and the absolute path to your CSV file:

    $ java -jar csv-to-sstable.jar <keyspace> <absolute/path/to/schema.cql> <absolute/path/to/input.csv> <absolute/path/to/output/dir> [optional csv prefs]

Optionally, you can pass CSV Preferences in JSON format. Omitting this parameter is equivalent to passing the following default preferences:
    
    $ java -jar csv-to-sstable.jar <keyspace> <absolute/path/to/schema.cql> <absolute/path/to/input.csv> <absolute/path/to/output/dir> "{\"col_sep\":\",\", \"quote_char\":\"'\"}"

Note that the quotes in your JSON must be escaped (as in the example above) in order to be passed on the command line.

## Example

Suppose you would like to populate a Cassandra table `my_table` within a keyspace `my_keyspace`.  The CQL definition of `my_table` is stored in `~/my_table.cql` and might look like this:

```cql
CREATE TABLE my_keyspace.my_table (
    my_column1 text,
    my_column2 int,
    my_column3 set<text>,
    PRIMARY KEY (my_column2, my_column1)
);
```

Assuming you want to use a CSV file `my_csv.csv` in your home directory to populate your table, you can run:

    $ java -jar csv-to-sstable.jar my_keyspace /Users/home/my_table.cql /Users/home/my_csv.csv /Users/home/sstables


After the SSTables have been generated, you can bulkload them into Cassandra by using sstableloader. Assuming that you have a running local Cassandra cluster installed in ~/cassandra, run:

    $ ~/cassandra/bin/sstableloader -d localhost Users/home/sstables/my_keyspace/my_table

## Supported column types:

Currently, only a limited number of column types are supported:

Cassandra column type  | Example CSV column
---------------------- | --------------------
text   | 'My Little Text'
float  | '8.97'
int    | '3'
boolean | 'True'
set<text> | '["first", "second", "third"]'


## Contributing

1. Fork it ( https://github.com/SPBTV/csv-to-sstable/fork )
2. Create your feature branch (`git checkout -b feature/my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/my-new-feature`)
5. Create a new Pull Request

##License

Copyright 2015 SPB TV AG

Licensed under the Apache License, Version 2.0 (the ["License"](LICENSE)); you may not use this file except in compliance with the License.

You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

See the License for the specific language governing permissions and limitations under the License.