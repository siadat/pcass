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
nvim -d cassandra_data_history/*/result.txt
```

## Varint

See cassandra-varint.md
