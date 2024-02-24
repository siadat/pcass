![alt text](https://github.com/siadat/public/blob/main/bytes.png)

## Running

- (Delete old Cassandra data files, if it is the second+ time running)
- Start Cassandra
- Wait for Cassandra to be ready
- Execute CQL statements from a file
- Stop Cassandra (to make sure everything is written to file)
- Copy Cassandra's data directory into a directory to work with

```
make populate_db
```

- Run parser on Cassandra data

```
make parse
```

- Open or compare the results

```
nvim -d cassandra_data_history/*/sina_test/*/result.txt

# or rename the dirs and compare specific version:
nvim -d cassandra_data_history/*{-simple,-simple-many-rows,-with-age}/sina_test/*/result.txt

# or one specific datafile:
nvim cassandra_data_history/2023-12-07_19-02-14-721636603-has-all-types/sina_test/*/result.txt
```

Run tests:
```
make test
```

## SSTable versions

About the SSTable "Big" versions (me, mc, etc):

* Cassandra 3.0 https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java#L130
* Cassandra 5.0 https://github.com/apache/cassandra/blob/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java#L348

Just for fun:
* Search all versions, just for fun: https://sourcegraph.com/search?q=context:global+repo:%5Egithub%5C.com/apache/cassandra%24+rev:*refs/tags/cassandra-*+content:%22public+static+final+String+current_version+%3D%22&patternType=standard&sm=1&groupBy=path
* Search for a specific version: https://sourcegraph.com/search?q=context:global+repo:%5Egithub%5C.com/apache/cassandra%24+rev:*refs/tags/cassandra-*+%28content:%27public+static+final+String+current_version+%3D+%22mc%22%27+OR+content:%27public+static+final+String+current_version+%3D+VERSION_MC%27%29&patternType=regexp&sm=1&groupBy=path

## Varint

See cassandra-varint.md
