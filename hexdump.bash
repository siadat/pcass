# print one byte (and its ascii representation) per line
hexdump -v -e '1/1 "0x%02x "' -e '1/1 "%_p\n"' "$@"
# dir="$1"
# hexdump -v -e '1/1 "0x%02x "' -e '1/1 "%_p\n"' cassandra_data_history/${dir}sina_test/*/me-1-big-Data.db
