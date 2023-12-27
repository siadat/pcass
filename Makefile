POETRY := /home/linuxbrew/.linuxbrew/bin/poetry
test:
	$(POETRY) env use python3.10
	$(POETRY) run python -m cql_struct
	$(POETRY) run pytest -x -s --cov=. --cov-report=html -v
	$(POETRY) run python -m sstable.import | jq -s 'if length != 2 then error("Length is not 2, it is \(length)") else "2 rows dumped" end'
	$(POETRY) run python -m sstable.dump test_data/cassandra3_data_want/sina_test/sina_table-*/ | jq -s 'if length != 7 then error("Length is not 7, it is \(length)") else "7 rows dumped" end'
	$(POETRY) run python -m sstable.dump test_data/cassandra3_data_want/sina_test/has_all_types-*/ | jq -s 'if length != 5 then error("Length is not 5, it is \(length)") else "5 rows dumped" end'

cql_server:
	$(POETRY) run python -m cql_server

cql_client:
	$(POETRY) run python -m cql_client

serve-coverage:
	cd htmlcov && $(POETRY) run python -m http.server

old_parse_all: generate_parser parse

parse:
	bash parse_parallel.bash

apache-cassandra-3.0.29:
	wget 'https://dlcdn.apache.org/cassandra/3.0.29/apache-cassandra-3.0.29-bin.tar.gz'
	tar xvzf apache-cassandra-3.0.29-bin.tar.gz
	rm apache-cassandra-3.0.29-bin.tar.gz

generate_parser: vlq_base128_le.ksy vlq_base128_be.ksy
	kaitai-struct-compiler --target python --opaque-types=true sstable-data-2.0.ksy

all: populate_db old_parse_all

.PHONY: populate_db
populate_db: clean cass_zig
	make populate_rows
	docker stop cass_zig

	rm -rf test_data/cassandra3_data_want/
	mkdir -p test_data/cassandra3_data_want/
	cp -rp cassandra_data/data/* test_data/cassandra3_data_want/
	cp populate_test_schema.cql test_data/cassandra3_data_want/

# ====

vlq_base128_le.ksy:
	wget https://raw.githubusercontent.com/kaitai-io/kaitai_struct_formats/master/common/vlq_base128_le.ksy

vlq_base128_be.ksy:
	wget https://raw.githubusercontent.com/kaitai-io/kaitai_struct_formats/master/common/vlq_base128_be.ksy

.PHONY: cass_zig
cass_zig:
	docker run -d \
		-v $(PWD)/cassandra-3.0.yaml:/etc/cassandra/cassandra.yaml \
		-v $(PWD)/:/root/work:ro \
		-v $(PWD)/cassandra_data:/var/lib/cassandra \
		-p 9042:9042 \
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

