.PHONY: cass4_zig
cass4_zig: clean
	docker run -d \
		-v $(PWD)/cassandra.yaml:/etc/cassandra/cassandra.yaml \
		-v $(PWD)/:/root/work:ro \
		-v $(PWD)/cassandra_data:/var/lib/cassandra \
		--name cass4_zig cassandra:4.1.3
	make populate_rows

.PHONY: populate_rows
populate_rows:
	docker exec -it cass4_zig /root/work/startup.bash

.PHONY: stop
stop:
	docker stop -f cass4_zig

.PHONY: clean
clean:
	docker rm -f cass4_zig
	rm -rf ./cassandra_data

.PHONY: bash
bash:
	docker exec -it cass4_zig bash

.PHONY: logs
logs:
	docker logs -f cass4_zig

.PHONY: consume
consume:
	docker run -v $(PWD)/:/src/ -v $(PWD)/cdc_raw/:/cdc_raw/ --rm -it groovy:latest bash
	# docker run -v $(PWD)/:/src/ --rm -it groovy:latest groovy /src/read-commitlog.groovy

