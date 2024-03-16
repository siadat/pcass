echo "Waiting for Cassandra to start listening on 9042..."
while ! netstat -tuln | grep -q 9042; do
	sleep 0.1
done
echo "Cassandra has started."
