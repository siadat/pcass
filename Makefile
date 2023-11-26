parse_all: generate_parser parse

parse:
	@for dir in cassandra_data_history/* ; do \
		bash parse.bash $$dir | tee $$dir/result.txt ; \
	done

generate_parser: vlq_base128_le.ksy
	kaitai-struct-compiler --target python sstable-data-2.0.ksy

.PHONY: generate_data
generate_data: clean cass_zig
	make populate_rows
	docker stop cass_zig

	mkdir -p cassandra_data_history/
	$(eval data_dir := cassandra_data_history/$(shell date "+%Y-%m-%d_%H-%M-%S-%N"))
	cp -rp cassandra_data/data $(data_dir)
	cp populate_rows.cql $(data_dir)

# ====

vlq_base128_le.ksy:
	wget https://raw.githubusercontent.com/kaitai-io/kaitai_struct_formats/master/common/vlq_base128_le.ksy

.PHONY: cass_zig
cass_zig:
	docker run -d \
		-v $(PWD)/cassandra-3.0.yaml:/etc/cassandra/cassandra.yaml \
		-v $(PWD)/:/root/work:ro \
		-v $(PWD)/cassandra_data:/var/lib/cassandra \
		--name cass_zig cassandra:3.0 || docker start cass_zig

.PHONY: populate_rows
populate_rows:
	docker exec -it cass_zig /root/work/startup.bash

.PHONY: stop
stop:
	docker stop -f cass_zig

.PHONY: clean
clean:
	docker rm -f cass_zig
	sudo rm -rf ./cassandra_data

.PHONY: bash
bash:
	docker exec -it cass_zig bash

.PHONY: logs
logs:
	docker logs -f cass_zig

.PHONY: consume
consume:
	docker run -v $(PWD)/:/src/ -v $(PWD)/cdc_raw/:/cdc_raw/ --rm -it groovy:latest bash
	# docker run -v $(PWD)/:/src/ --rm -it groovy:latest groovy /src/read-commitlog.groovy

