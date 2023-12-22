CREATE KEYSPACE sina_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE sina_test;

-- CREATE TYPE example_user_type2 (
--     height float
-- );
-- CREATE TYPE example_user_type (
--     name text,
--     age int,
--     metadata frozen<example_user_type2>
-- );
-- CREATE TABLE collection_types (
--     k int PRIMARY KEY,
--     l list<int>,
--     s set<int>,
--     m map<int, int>,
-- 
--     simple_frozen frozen<list<int>>,
-- 
--     f frozen<example_user_type>,
--     lf list<frozen<example_user_type>>,
--     flf frozen<list<frozen<example_user_type>>>
-- ) WITH compression = {'enabled':'false'};
-- 
-- 
-- INSERT INTO collection_types (k, l, s, m, simple_frozen, f, lf, flf) VALUES (0, [1, 2, 3], {4, 5, 6}, {7: 8, 9: 10}, [11, 12, 13], {name: 'foo', age: 42, metadata: {height: 1.83}}, [{name: 'bar', age: 24, metadata: {height: 1.82}}, {name: 'baz', age: 66, metadata: {height: 1.81}}], [{name: 'qux', age: 88, metadata: {height: 1.80}}, {name: 'quux', age: 99, metadata: {height: 1.79}}]);

CREATE TABLE table_with_map (
    k int PRIMARY KEY,
    m map<int, int>,
) WITH compression = {'enabled':'false'};

INSERT INTO table_with_map (k, m) VALUES (0, {1: 2, 3: 4});
INSERT INTO table_with_map (k, m) VALUES (1, {10: 20, 30: 40});

CREATE TABLE table_with_list (
    k int PRIMARY KEY,
    l list<int>,
) WITH compression = {'enabled':'false'};

INSERT INTO table_with_list (k, l) VALUES (0, [1, 2, 3]);
INSERT INTO table_with_list (k, l) VALUES (1, [4, 5, 6]);


CREATE TABLE sina_table (
  id int,
  name text,
  aboutme text,
  gender text,
  age int,
  col1 int, col2 int, col3 int, col4 int, col5 int, col6 int, col7 int, col8 int,
  col9 int, col10 int, col11 int, col12 int, col13 int, col14 int, col15 int, col16 int,
  col17 int, col18 int, col19 int, col20 int, col21 int, col22 int, col23 int, col24 int,
  col25 int, col26 int, col27 int, col28 int, col29 int, col30 int, col31 int, col32 int,
  col33 int, col34 int, col35 int, col36 int, col37 int, col38 int, col39 int, col40 int,
  col41 int, col42 int, col43 int, col44 int, col45 int, col46 int, col47 int, col48 int,
  col49 int, col50 int, col51 int, col52 int, col53 int, col54 int, col55 int, col56 int,
  col57 int, col58 int, col59 int, col60 int, col61 int, col62 int, col63 int, col64 int,
  PRIMARY KEY ((id), name)
) WITH compression = {'enabled':'false'};

INSERT INTO sina_table (id, name,          gender, age          ) VALUES ( 1, 'sina', 'male', 39);
INSERT INTO sina_table (id, name,          gender               ) VALUES ( 2, 'soheil', 'male');
INSERT INTO sina_table (id, name, aboutme, gender, age,
      col2, col3, col4, col5, col6, col7, col8,
col9, col10, col11, col12, col13, col14, col15, col16,
col17, col18, col19, col20, col21, col22, col23, col24,
col25, col26, col27, col28, col29, col30, col31, col32,
col33, col34, col35, col36, col37, col38, col39, col40,
col41, col42, col43, col44, col45, col46, col47, col48,
col49, col50, col51, col52, col53, col54, col55, col56,
col57, col58, col59, col60, col61, col62, col63, col64
) VALUES ( 3, 'sara', 'hi my name is sara!', 'female', 44,
      2, 3, 4, 5, 6, 7, 8,
9, 10, 11, 12, 13, 14, 15, 16,
17, 18, 19, 20, 21, 22, 23, 24,
25, 26, 27, 28, 29, 30, 31, 32,
33, 34, 35, 36, 37, 38, 39, 40,
41, 42, 43, 44, 45, 46, 47, 48,
49, 50, 51, 52, 53, 54, 55, 56,
57, 58, 59, 60, 61, 62, 63, 64
);
INSERT INTO sina_table (id, name, aboutme                       ) VALUES ( 4, 'mama', 'hi my name is mama!');
INSERT INTO sina_table (id, name                                ) VALUES ( 5, 'baba');
INSERT INTO sina_table (id, name                      , col4) VALUES ( 6, 'ordak', 42);
INSERT INTO sina_table (id, name                                              , col11) VALUES ( 7, 'boo', 100);

-- https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/pylib/cqlshlib/test/test_keyspace_init.cql
CREATE TABLE has_all_types (
    num int PRIMARY KEY,
    intcol int,
    asciicol ascii,
    bigintcol bigint,
    blobcol blob,
    booleancol boolean,
    decimalcol decimal,
    doublecol double,
    floatcol float,
    smallintcol smallint,
    textcol text,
    timestampcol timestamp,
    tinyintcol tinyint,
    uuidcol uuid,
    varcharcol varchar,
    varintcol varint
) WITH compression = {'enabled':'false'};

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (0, -12, 'abcdefg', 1234567890123456789, 0x000102030405fffefd, true,
        19952.11882, 1.0, -2.1, 32767, 'Voilá!',
        '2012-05-14 12:53:20+0000', 127, bd1924e1-6af8-44ae-b5e1-f24131dbd460, '"', 10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (1, 2147483647, '__!''$#@!~"', 9223372036854775807, 0xffffffffffffffffff, true,
        0.00000000000001, 9999999.999, 99999.999, 32767, '∭Ƕ⑮ฑ➳❏''',
        '1950-01-01+0000', 127, ffffffff-ffff-ffff-ffff-ffffffffffff, 'newline->
<-', 9);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (2, 0, '', 0, 0x, false,
        0.0, 0.0, 0.0, 0, '',
        0, 0, 00000000-0000-0000-0000-000000000000, '', 0);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (3, -2147483648, '''''''', -9223372036854775808, 0x80, false,
        10.0000000000000, -1004.10, 100000000.9, 32767, '龍馭鬱',
        '2038-01-19T03:14-1200', 127, ffffffff-ffff-1fff-8fff-ffffffffffff,
	    '''', -10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (4, blobAsInt(0x), '', blobAsBigint(0x), 0x, blobAsBoolean(0x),
	blobAsDecimal(0x), blobAsDouble(0x), blobAsFloat(0x), blobAsSmallInt(0x0000), '',
	blobAsTimestamp(0x), blobAsTinyInt(0x00), blobAsUuid(0x), '', blobAsVarint(0x));



CREATE TABLE empty_table (
    lonelykey float primary key,
    lonelycol text
) WITH compression = {'enabled':'false'};



CREATE COLUMNFAMILY dynamic_columns (
    somekey int,
    column1 float,
    value text,
    PRIMARY KEY(somekey, column1)
) WITH COMPACT STORAGE AND compression ={'enabled':'false'};

INSERT INTO dynamic_columns (somekey, column1, value) VALUES (1, 1.2, 'one point two');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (2, 2.3, 'two point three');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (3, 3.46, 'three point four six');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (3, 99.0, 'ninety-nine point oh');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (3, -0.0001, 'negative ten thousandth');



CREATE TABLE twenty_rows_table (
    a text primary key,
    b text
) WITH compression = {'enabled':'false'};

INSERT INTO twenty_rows_table (a, b) VALUES ('1', '1');
INSERT INTO twenty_rows_table (a, b) VALUES ('2', '2');
INSERT INTO twenty_rows_table (a, b) VALUES ('3', '3');
INSERT INTO twenty_rows_table (a, b) VALUES ('4', '4');
INSERT INTO twenty_rows_table (a, b) VALUES ('5', '5');
INSERT INTO twenty_rows_table (a, b) VALUES ('6', '6');
INSERT INTO twenty_rows_table (a, b) VALUES ('7', '7');
INSERT INTO twenty_rows_table (a, b) VALUES ('8', '8');
INSERT INTO twenty_rows_table (a, b) VALUES ('9', '9');
INSERT INTO twenty_rows_table (a, b) VALUES ('10', '10');
INSERT INTO twenty_rows_table (a, b) VALUES ('11', '11');
INSERT INTO twenty_rows_table (a, b) VALUES ('12', '12');
INSERT INTO twenty_rows_table (a, b) VALUES ('13', '13');
INSERT INTO twenty_rows_table (a, b) VALUES ('14', '14');
INSERT INTO twenty_rows_table (a, b) VALUES ('15', '15');
INSERT INTO twenty_rows_table (a, b) VALUES ('16', '16');
INSERT INTO twenty_rows_table (a, b) VALUES ('17', '17');
INSERT INTO twenty_rows_table (a, b) VALUES ('18', '18');
INSERT INTO twenty_rows_table (a, b) VALUES ('19', '19');
INSERT INTO twenty_rows_table (a, b) VALUES ('20', '20');


CREATE TABLE undefined_values_table (
    k text PRIMARY KEY,
    c text,
    notthere text
) WITH compression = {'enabled':'false'};

INSERT INTO undefined_values_table (k, c) VALUES ('k1', 'c1');
INSERT INTO undefined_values_table (k, c) VALUES ('k2', 'c2');



CREATE TABLE ascii_with_special_chars (
    k int PRIMARY KEY,
    val ascii
) WITH compression = {'enabled':'false'};

-- "newline:\n"
INSERT INTO ascii_with_special_chars (k, val) VALUES (0, blobAsAscii(0x6e65776c696e653a0a));
-- "return\rand null\0!"
INSERT INTO ascii_with_special_chars (k, val) VALUES (1, blobAsAscii(0x72657475726e0d616e64206e756c6c0021));
-- "\x00\x01\x02\x03\x04\x05control chars\x06\x07"
INSERT INTO ascii_with_special_chars (k, val) VALUES (2, blobAsAscii(0x000102030405636f6e74726f6c2063686172730607));
-- "fake special chars\\x00\\n"
INSERT INTO ascii_with_special_chars (k, val) VALUES (3, blobAsAscii(0x66616b65207370656369616c2063686172735c7830305c6e));



CREATE TABLE utf8_with_special_chars (
    k int PRIMARY KEY,
    val text
) WITH compression = {'enabled':'false'};

INSERT INTO utf8_with_special_chars (k, val) VALUES (0, 'Normal string');
INSERT INTO utf8_with_special_chars (k, val) VALUES (1, 'Text with
newlines
');
INSERT INTO utf8_with_special_chars (k, val) VALUES (2, 'Text with embedded  char');
INSERT INTO utf8_with_special_chars (k, val) VALUES (3, 'ⓈⓅⒺⒸⒾⒶⓁ ⒞⒣⒜⒭⒮ and normal ones');
INSERT INTO utf8_with_special_chars (k, val) VALUES (4, 'double wides: ⾑⾤⾚');
INSERT INTO utf8_with_special_chars (k, val) VALUES (5, 'zero width​space');
INSERT INTO utf8_with_special_chars (k, val) VALUES (6, 'fake special chars\x00\n');


CREATE TABLE empty_composite_table (
    lonelykey float,
    lonelycol text,
    lonelyval int,
    primary key (lonelykey, lonelycol)
) WITH compression = {'enabled':'false'};

CREATE TABLE twenty_rows_composite_table (
    a text,
    b text,
    c text,
    primary key (a, b)
) WITH compression = {'enabled':'false'};

-- all in the same storage engine row:

INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '1', '1');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '2', '2');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '3', '3');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '4', '4');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '5', '5');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '6', '6');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '7', '7');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '8', '8');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '9', '9');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '10', '10');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '11', '11');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '12', '12');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '13', '13');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '14', '14');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '15', '15');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '16', '16');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '17', '17');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '18', '18');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '19', '19');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '20', '20');

CREATE TYPE address (
    city text,
    address text,
    zip text
);

CREATE TYPE phone_number (
    country text,
    number text
);

CREATE TABLE users (
    login text PRIMARY KEY,
    name text,
    addresses set<frozen<address>>,
    phone_numbers set<frozen<phone_number>>
) WITH compression = {'enabled':'false'};

insert into users (login, name, addresses, phone_numbers)
values ('jbellis',
        'jonathan ellis',
        {{city: 'Austin', address: '902 East 5th St. #202', zip: '78702'},
         {city: 'Sunnyvale', address: '292 Gibraltar Drive #107', zip: '94089'}},
        {{country: '+44', number: '208 622 3021'},
         {country: '+1', number: '512-537-7809'}});

insert into users (login, name, addresses, phone_numbers)
values ('vpupkin',
        'vasya pupkin',
        {{city: 'Chelyabinsk', address: '3rd street', zip: null},
         {city: 'Chigirinsk', address: null, zip: '676722'}},
        {{country: '+7', number: null},
         {country: null, number: '03'}});

CREATE TYPE band_info_type (
  founded varint,
  members set<text>,
  description text
);

CREATE TYPE tags (
  tags map<text, text>
);

CREATE TABLE songs (
    title text PRIMARY KEY,
    band text,
    info frozen<band_info_type>,
    tags frozen<tags>
) WITH compression = {'enabled':'false'};

insert into songs (title, band, info, tags)
values (
    'The trooper',
    'Iron Maiden',
    {
        founded:188694000,
        members: {
            'Bruce Dickinson',
            'Dave Murray',
            'Adrian Smith',
            'Janick Gers',
            'Steve Harris',
            'Nicko McBrain'
        },
        description: 'Pure evil metal'
     },
    {
        tags: {
            'genre':'metal',
            'origin':'england'
        }
    });

