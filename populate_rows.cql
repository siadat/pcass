CREATE KEYSPACE IF NOT EXISTS sina_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS sina_test.my_table ( id int, name text, aboutme text, age int, PRIMARY KEY ((id), name)) WITH compression = {'enabled':'false'};

-- cdc is not available in Cassandra 3.0
-- I get "populate_rows.cql:4:SyntaxException: Unknown property 'cdc'"
-- ALTER TABLE sina_test.my_table WITH cdc=true;

INSERT INTO sina_test.my_table (id, name, aboutme, age) VALUES ( 1, 'sina', 'hi my name is sina!', 39);
INSERT INTO sina_test.my_table (id, name, aboutme, age) VALUES ( 2, 'soheil', 'hi my name is soheil!', 46);
INSERT INTO sina_test.my_table (id, name, aboutme, age) VALUES ( 3, 'sara', 'hi my name is sara!', 44);

SELECT COUNT(*) FROM sina_test.my_table;
