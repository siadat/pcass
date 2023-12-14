#!/bin/bash
set -e

data_dir=./test_data/cassandra3_data_want
keyspace_name=sina_test
rm -rf ./test_data/parsed/
for table_dir in $data_dir/${keyspace_name}/* ; do
  table_name=$(basename $table_dir | cut -d'-' -f1)
  table_parsed_dir=./test_data/parsed/$keyspace_name/$table_name
  mkdir -p $table_parsed_dir

  for data_db_file in $table_dir/me-*-big-Data.db; do
    if [ ! -e $data_db_file ]; then
      continue
    fi
    echo "Parsing table ${data_db_file}"
    {
      poetry run python -m parse_with_construct "${table_dir}" > $table_parsed_dir/$(basename $data_db_file).hex
      echo "Done ${data_db_file}"
    } &
  done
done
wait
