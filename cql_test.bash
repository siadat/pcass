make cql_server &
server_pid="$!"

sleep 2
make cql_client

kill $server_pid
echo

fg
