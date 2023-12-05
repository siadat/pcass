set -e
dir="$1"
echo ----
echo "$dir"
cat "${dir}"/populate_rows.cql | grep -v '^$' | grep -v -P '^--' | nl

# poetry run python parse_with_kaitai.py "${dir}"/sina_test/my_table-*/me-1-big-Data.db
poetry run python parse_with_construct.py "${dir}"/sina_test/my_table-*/
