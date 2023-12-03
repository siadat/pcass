## Cracking the code

This is one of the files in which I was trying to decode the binary format.
I generate several files like this and compare them.
I also edited this one by adding newlines and comment starting with "#" while trying to fugure out the format and comparing rows and cells and partitions.
The "# Parsed to here." comment is printed by the parser.
This was one of the last important bits for decoding this Data.db file, after this iteration I was able to parse the whole Data.db file.
I decided to keep one of them to help me remember the process and to celebrate it. :)

```
----
cassandra_data_history/2023-11-26_12-19-10-023865125
     1	CREATE KEYSPACE IF NOT EXISTS sina_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
     2	CREATE TABLE IF NOT EXISTS sina_test.my_table ( id int, name text, aboutme text, PRIMARY KEY ((id), name)) WITH compression = {'enabled':'false'};
     3	INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 1, 'sina', 'hi my name is sina!');
     4	INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 2, 'soheil', 'hi my name is soheil!');
     5	INSERT INTO sina_test.my_table (id, name, aboutme) VALUES ( 3, 'sara', 'hi my name is sara!');
     6	SELECT COUNT(*) FROM sina_test.my_table;
Container: 
    partition_header = Container: 
        key_len = 4
        key = unhexlify('00000001')
        deletion_time = Container: 
            local_deletion_time = 2147483647
            marked_for_delete_at = 9223372036854775808
    unfiltered = Container: 
        row_flags = 0x24
        row = Container: 
            clustering_block = Container: 
                clustering_block_header = 0
                clustering_cells = Container: 
                    cell_value_len = 4
                    cell_value = b'sina' (total 4)
            cells = Container: 
0x00	b00000000	  0	─── (parsing) -> partition_header -> key_len
0x04	b00000100	  4	─── (parsing) -> partition_header -> key_len
0x00	b00000000	  0	─── (parsing) -> partition_header -> key
0x00	b00000000	  0	─── (parsing) -> partition_header -> key
0x00	b00000000	  0	─── (parsing) -> partition_header -> key
0x01	b00000001	  1	─── (parsing) -> partition_header -> key
0x7f	b01111111	127	─── (parsing) -> partition_header -> deletion_time -> local_deletion_time
0xff	b11111111	255	─── (parsing) -> partition_header -> deletion_time -> local_deletion_time
0xff	b11111111	255	─── (parsing) -> partition_header -> deletion_time -> local_deletion_time
0xff	b11111111	255	─── (parsing) -> partition_header -> deletion_time -> local_deletion_time
0x80	b10000000	128	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x00	b00000000	  0	─── (parsing) -> partition_header -> deletion_time -> marked_for_delete_at
0x24	b00100100	 36	'$' (parsing) -> unfiltered -> row_flags
0x00	b00000000	  0	─── (parsing) -> unfiltered -> row -> clustering_block -> clustering_block_header
0x04	b00000100	  4	─── (parsing) -> unfiltered -> row -> clustering_block -> clustering_cells -> cell_value_len
0x73	b01110011	115	's' (parsing) -> unfiltered -> row -> clustering_block -> clustering_cells -> cell_value
0x69	b01101001	105	'i' (parsing) -> unfiltered -> row -> clustering_block -> clustering_cells -> cell_value
0x6e	b01101110	110	'n' (parsing) -> unfiltered -> row -> clustering_block -> clustering_cells -> cell_value
0x61	b01100001	 97	'a' (parsing) -> unfiltered -> row -> clustering_block -> clustering_cells -> cell_value
# Parsed to here.

# row size for skipping 1
0x17	b00010111	 23	───
0x12	b00010010	 18	───

# timestamp diff 1:
0x00	b00000000	  0	───

# cell flag 1:
0x08	b00001000	  8	───

0x13	b00010011	 19	───
0x68	b01101000	104	'h'
0x69	b01101001	105	'i'
0x20	b00100000	 32	' '
0x6d	b01101101	109	'm'
0x79	b01111001	121	'y'
0x20	b00100000	 32	' '
0x6e	b01101110	110	'n'
0x61	b01100001	 97	'a'
0x6d	b01101101	109	'm'
0x65	b01100101	101	'e'
0x20	b00100000	 32	' '
0x69	b01101001	105	'i'
0x73	b01110011	115	's'
0x20	b00100000	 32	' '
0x73	b01110011	115	's'
0x69	b01101001	105	'i'
0x6e	b01101110	110	'n'
0x61	b01100001	 97	'a'
0x21	b00100001	 33	'!'

0x01	b00000001	  1	───

partition_key_size:
0x00	b00000000	  0	───
0x04	b00000100	  4	───

partition_key 2:
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x02	b00000010	  2	───

0x7f	b01111111	127	───
0xff	b11111111	255	───
0xff	b11111111	255	───
0xff	b11111111	255	───
0x80	b10000000	128	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───

0x24	b00100100	 36	'$'
0x00	b00000000	  0	───
0x06	b00000110	  6	───
0x73	b01110011	115	's'
0x6f	b01101111	111	'o'
0x68	b01101000	104	'h'
0x65	b01100101	101	'e'
0x69	b01101001	105	'i'
0x6c	b01101100	108	'l'

# row size for skipping 2
0x1b	b00011011	 27	───
0x12	b00010010	 18	───

# timestamp diff 2:
0xc0	b11000000	192	───
0x76	b01110110	118	'v'
0x1d	b00011101	 29	───

# cell flag 2:
0x08	b00001000	  8	───

0x15	b00010101	 21	───
0x68	b01101000	104	'h'
0x69	b01101001	105	'i'
0x20	b00100000	 32	' '
0x6d	b01101101	109	'm'
0x79	b01111001	121	'y'
0x20	b00100000	 32	' '
0x6e	b01101110	110	'n'
0x61	b01100001	 97	'a'
0x6d	b01101101	109	'm'
0x65	b01100101	101	'e'
0x20	b00100000	 32	' '
0x69	b01101001	105	'i'
0x73	b01110011	115	's'
0x20	b00100000	 32	' '
0x73	b01110011	115	's'
0x6f	b01101111	111	'o'
0x68	b01101000	104	'h'
0x65	b01100101	101	'e'
0x69	b01101001	105	'i'
0x6c	b01101100	108	'l'
0x21	b00100001	 33	'!'

0x01	b00000001	  1	───

partition_key_size:
0x00	b00000000	  0	───
0x04	b00000100	  4	───

partition_key 3:
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x03	b00000011	  3	───

0x7f	b01111111	127	───
0xff	b11111111	255	───
0xff	b11111111	255	───
0xff	b11111111	255	───
0x80	b10000000	128	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───
0x00	b00000000	  0	───

0x24	b00100100	 36	'$'
0x00	b00000000	  0	───
0x04	b00000100	  4	───
0x73	b01110011	115	's'
0x61	b01100001	 97	'a'
0x72	b01110010	114	'r'
0x61	b01100001	 97	'a'

# row size for skipping 3
0x19	b00011001	 25	───
0x12	b00010010	 18	───

# timestamp diff 3:
0xc0	b11000000	192	───
0x8e	b10001110	142	───
0x67	b01100111	103	'g'

# cell flag 3:
0x08	b00001000	  8	───

0x13	b00010011	 19	───
0x68	b01101000	104	'h'
0x69	b01101001	105	'i'
0x20	b00100000	 32	' '
0x6d	b01101101	109	'm'
0x79	b01111001	121	'y'
0x20	b00100000	 32	' '
0x6e	b01101110	110	'n'
0x61	b01100001	 97	'a'
0x6d	b01101101	109	'm'
0x65	b01100101	101	'e'
0x20	b00100000	 32	' '
0x69	b01101001	105	'i'
0x73	b01110011	115	's'
0x20	b00100000	 32	' '
0x73	b01110011	115	's'
0x61	b01100001	 97	'a'
0x72	b01110010	114	'r'
0x61	b01100001	 97	'a'
0x21	b00100001	 33	'!'

0x01	b00000001	  1	───
```
