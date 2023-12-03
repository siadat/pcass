#!/bin/bash
for dir in cassandra_data_history/* ; do
	./hexdump.bash "$dir"/sina_test/*/me-1-big-Data.db > "$dir"/bytes.txt &
	# bash parse.bash "$dir" > "$dir"/result.txt &
	bash parse.bash "$dir" | tee "$dir"/result.txt &
done
wait
