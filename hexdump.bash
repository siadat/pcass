# print one byte (and its ascii representation) per line
hexdump -v -e '1/1 "0x%02x "' -e '1/1 "%_p\n"' "$@"
