set -e
dir="$1"
echo ----
echo "$dir"
cat "${dir}"/populate_rows.cql | grep -v '^$' | grep -v -P '^--' | nl
poetry run python parse.py "${dir}"/sina_test/my_table-*/me-1-big-Data.db
