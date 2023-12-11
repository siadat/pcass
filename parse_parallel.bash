#!/bin/bash
set -e
set -x

for data_dir in cassandra_data_history/*-with-age ; do
  cat "${data_dir}"/populate_rows.cql | grep -v '^$' | grep -v -P '^--' | nl > "$data_dir"/populate_rows.cql
  for table_dir in $data_dir/sina_test/* ; do

    for statistics_db_file in $table_dir/me-*-big-Statistics.db; do
      if [ ! -e $statistics_db_file ]; then
        continue
      fi
      ./hexdump.bash $statistics_db_file > ${statistics_db_file}.txt
    done

    for data_db_file in $table_dir/me-*-big-Data.db; do
      if [ ! -e $data_db_file ]; then
        continue
      fi
      echo "Parsing table ${data_db_file}"
      ./hexdump.bash $data_db_file > ${data_db_file}.txt
      {
        poetry run python parse_with_construct.py "${table_dir}" > "${data_db_file}-result.txt"
        echo "Done ${data_db_file}"
      } # &
    done
  done
done
wait
