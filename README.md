
## How to run

![tests](https://github.com/siadat/pcass/actions/workflows/makefile.yml/badge.svg)

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

## SSTable dump example

This example parses and dumps the SSTable files in
[sina_test/has_all_types-9071b940a1c711eeae8c6d2c86545d91/](https://github.com/siadat/pcass/tree/ab737266c6ba846a568ac599a0b7fdf6a0b4488b/test_data/cassandra3_data_want/sina_test/has_all_types-9071b940a1c711eeae8c6d2c86545d91).
Here's the byte-by-byte representation of the parsed [Statistics.db](https://github.com/siadat/pcass/blob/ab737266c6ba846a568ac599a0b7fdf6a0b4488b/test_data/parsed/sina_test/has_all_types/me-1-big-Data.db.hex#L986)
and [Data.db](https://github.com/siadat/pcass/blob/ab737266c6ba846a568ac599a0b7fdf6a0b4488b/test_data/parsed/sina_test/has_all_types/me-1-big-Data.db.hex#L6436).

```
poetry run python -m sstable.dump test_data/cassandra3_data_want/sina_test/has_all_types-*/ | jq .
```
```json
{
  "partition_key_value": "00000001",
  "cells": [
    {
      "name": "asciicol",
      "value": "__!'$#@!~\""
    },
    {
      "name": "bigintcol",
      "value": 9223372036854775807
    },
    {
      "name": "blobcol",
      "value": "ffffffffffffffffff"
    },
    {
      "name": "booleancol",
      "value": 1
    },
    {
      "name": "decimalcol",
      "value": 1E-14
    },
    {
      "name": "doublecol",
      "value": 9999999.999
    },
    {
      "name": "floatcol",
      "value": 100000.0
    },
    {
      "name": "intcol",
      "value": 2147483647
    },
    {
      "name": "smallintcol",
      "value": 32767
    },
    {
      "name": "textcol",
      "value": "∭Ƕ⑮ฑ➳❏'"
    },
    {
      "name": "timestampcol",
      "value": -631152000000
    },
    {
      "name": "tinyintcol",
      "value": "7f"
    },
    {
      "name": "uuidcol",
      "value": "ffffffffffffffffffffffffffffffff"
    },
    {
      "name": "varcharcol",
      "value": "newline->\n<-"
    },
    {
      "name": "varintcol",
      "value": 9
    }
  ]
}
{
  "partition_key_value": "00000000",
  "cells": [
    {
      "name": "asciicol",
      "value": "abcdefg"
    },
    {
      "name": "bigintcol",
      "value": 1234567890123456789
    },
    {
      "name": "blobcol",
      "value": "000102030405fffefd"
    },
    {
      "name": "booleancol",
      "value": 1
    },
    {
      "name": "decimalcol",
      "value": 19952.118820000003
    },
    {
      "name": "doublecol",
      "value": 1.0
    },
    {
      "name": "floatcol",
      "value": -2.0999999046325684
    },
    {
      "name": "intcol",
      "value": -12
    },
    {
      "name": "smallintcol",
      "value": 32767
    },
    {
      "name": "textcol",
      "value": "Voilá!"
    },
    {
      "name": "timestampcol",
      "value": 1337000000000
    },
    {
      "name": "tinyintcol",
      "value": "7f"
    },
    {
      "name": "uuidcol",
      "value": "bd1924e16af844aeb5e1f24131dbd460"
    },
    {
      "name": "varcharcol",
      "value": "\""
    },
    {
      "name": "varintcol",
      "value": 10000000000000000000000000
    }
  ]
}
{
  "partition_key_value": "00000002",
  "cells": [
    {
      "name": "asciicol",
      "value": null
    },
    {
      "name": "bigintcol",
      "value": 0
    },
    {
      "name": "blobcol",
      "value": null
    },
    {
      "name": "booleancol",
      "value": 0
    },
    {
      "name": "decimalcol",
      "value": 0.0
    },
    {
      "name": "doublecol",
      "value": 0.0
    },
    {
      "name": "floatcol",
      "value": 0.0
    },
    {
      "name": "intcol",
      "value": 0
    },
    {
      "name": "smallintcol",
      "value": 0
    },
    {
      "name": "textcol",
      "value": null
    },
    {
      "name": "timestampcol",
      "value": 0
    },
    {
      "name": "tinyintcol",
      "value": "00"
    },
    {
      "name": "uuidcol",
      "value": "00000000000000000000000000000000"
    },
    {
      "name": "varcharcol",
      "value": null
    },
    {
      "name": "varintcol",
      "value": 0
    }
  ]
}
{
  "partition_key_value": "00000004",
  "cells": [
    {
      "name": "asciicol",
      "value": null
    },
    {
      "name": "bigintcol",
      "value": null
    },
    {
      "name": "blobcol",
      "value": null
    },
    {
      "name": "booleancol",
      "value": null
    },
    {
      "name": "decimalcol",
      "value": null
    },
    {
      "name": "doublecol",
      "value": null
    },
    {
      "name": "floatcol",
      "value": null
    },
    {
      "name": "intcol",
      "value": null
    },
    {
      "name": "smallintcol",
      "value": 0
    },
    {
      "name": "textcol",
      "value": null
    },
    {
      "name": "timestampcol",
      "value": null
    },
    {
      "name": "tinyintcol",
      "value": "00"
    },
    {
      "name": "uuidcol",
      "value": null
    },
    {
      "name": "varcharcol",
      "value": null
    },
    {
      "name": "varintcol",
      "value": null
    }
  ]
}
{
  "partition_key_value": "00000003",
  "cells": [
    {
      "name": "asciicol",
      "value": "'''"
    },
    {
      "name": "bigintcol",
      "value": -9223372036854775808
    },
    {
      "name": "blobcol",
      "value": "80"
    },
    {
      "name": "booleancol",
      "value": 0
    },
    {
      "name": "decimalcol",
      "value": 10.0
    },
    {
      "name": "doublecol",
      "value": -1004.1
    },
    {
      "name": "floatcol",
      "value": 100000000.0
    },
    {
      "name": "intcol",
      "value": -2147483648
    },
    {
      "name": "smallintcol",
      "value": 32767
    },
    {
      "name": "textcol",
      "value": "龍馭鬱"
    },
    {
      "name": "timestampcol",
      "value": 2147526840000
    },
    {
      "name": "tinyintcol",
      "value": "7f"
    },
    {
      "name": "uuidcol",
      "value": "ffffffffffff1fff8fffffffffffffff"
    },
    {
      "name": "varcharcol",
      "value": "'"
    },
    {
      "name": "varintcol",
      "value": 299485009821345068724781056
    }
  ]
}
```

## Varint

See [./cassandra-varint.md](./cassandra-varint.md).

## Just for fun

![bits](https://github.com/siadat/public/blob/main/bytes.png)
