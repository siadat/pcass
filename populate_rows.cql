CREATE KEYSPACE IF NOT EXISTS sina_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS sina_test.my_table ( id int, name text, aboutme text, PRIMARY KEY ((id), name));
DELETE FROM sina_test.my_table WHERE id in (1, 2, 3);
INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 1, 'sina', 'hi my name is sina!');
INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 2, 'soheil', 'hi my name is soheil!');
INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 3, 'sara', 'hi my name is sara!');
