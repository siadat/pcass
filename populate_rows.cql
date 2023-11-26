CREATE KEYSPACE IF NOT EXISTS sina_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS sina_test.my_table ( id int, name text, aboutme text, PRIMARY KEY ((id), name)) WITH compression = {'enabled':'false'};

-- cdc is not available in Cassandra 3.0
-- I get "populate_rows.cql:4:SyntaxException: Unknown property 'cdc'"
-- ALTER TABLE sina_test.my_table WITH cdc=true;

INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 1, 'sina', 'hi my name is sina!');
INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 2, 'soheil', 'hi my name is soheil!');
INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 3, 'sara', 'hi my name is sara!');

SELECT COUNT(*) FROM sina_test.my_table;
