-- https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/pylib/cqlshlib/test/test_keyspace_init.cql

CREATE KEYSPACE sina_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE sina_test.has_all_types (
    num int,
    intcol int,
    asciicol ascii,
    PRIMARY KEY(num),
) WITH compression = {'enabled':'false'};

INSERT INTO sina_test.has_all_types (num, intcol, asciicol)
VALUES (0, -12, 'abcdefg');
