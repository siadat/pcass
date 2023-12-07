#!/bin/bash

for data_dir in cassandra_data_history/* ; do
  cat "${data_dir}"/populate_rows.cql | grep -v '^$' | grep -v -P '^--' | nl > "$data_dir"/cql.txt
  for table_dir in $data_dir/sina_test/* ; do
    for data_db_file in $table_dir/me-*-big-Data.db; do
      if [ ! -e $data_db_file ]; then
        continue
      fi
      echo "Parsing table ${data_db_file}"
      ./hexdump.bash $data_db_file > "$table_dir"/bytes.txt &
      poetry run python parse_with_construct.py "${table_dir}" >> "$table_dir"/result.txt &
    done
  done
  wait
done
