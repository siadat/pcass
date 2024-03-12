POETRY := poetry

.PHONY: test
test:
	$(POETRY) env use python3.10
	$(POETRY) run python -m cql_struct
	$(POETRY) run pytest -x -s --cov=. --cov-report=html -v
	$(POETRY) run python -m sstable.import | jq -s 'if length != 2 then error("Length is not 2, it is \(length)") else "2 rows dumped" end'
	$(POETRY) run python -m sstable.dump test_data/cassandra3_data_want/sina_test/sina_table-*/ | jq -s 'if length != 7 then error("Length is not 7, it is \(length)") else "7 rows dumped" end'
	$(POETRY) run python -m sstable.dump test_data/cassandra3_data_want/sina_test/has_all_types-*/ | jq -s 'if length != 5 then error("Length is not 5, it is \(length)") else "5 rows dumped" end'

.PHONY: install-dependencies
install-dependencies:
	# --no-root is used to avoid attempting to install this package itself,
	#  because it is not yet a package.
	$(POETRY) install --no-root

docker-compose-up:
	export SGTAG=v2 && \
	export CASSTAG=4.0 && \
	docker compose rm -f && \
	docker compose up

docker-compose-debug:
	export SGTAG=v2 && \
	export CASSTAG=4.0 && \
	docker compose exec debugger bash

docker-compose-restart-debug:
	export SGTAG=v2 && \
	export CASSTAG=4.0 && \
	docker compose up -d --no-deps --force-recreate debugger

test-zig:
	zig build test --summary all --verbose # --verbose-llvm-ir

zig-run:
	TRACY_NO_INVARIANT_CHECK=1 \
	TRACY_PORT=5454 \
	TRACY_CALLSTACK=1 \
	zig build run --verbose \
		-Dtracy=./tracy \
		-Dtracy-allocation \
		-Dtracy-callstack \
		--summary all

tracy.zig:
	# https://github.com/ziglang/zig/blob/aa7d16aba1f0b3a9e816684618d16cb1d178a6d3/src/tracy.zig
	wget https://raw.githubusercontent.com/ziglang/zig/aa7d16aba1f0b3a9e816684618d16cb1d178a6d3/src/tracy.zig

capture-tracy:
	@# First start the capture process
	@# Then start the TCP server
	@# Then feed input using nc
	@# (all processes are stopped now)
	@# Then download ~/public/trace.tracy and open in https://tracy.nereid.pl/
	LD_LIBRARY_PATH=./capstone ./tracy/capture/build/unix/capture-release -f -o ~/public/trace.tracy -a localhost -p 5454

test-tracy:
	sudo apt-get install libdebuginfod-dev
	cd tracy && make -C test/ clean all
	./test/tracy_test

send-test-bytes:
	yes 'hello world!' | head -c 20 | nc -N localhost 9042

.PHONY: got.lisp
got.lisp:
	poetry run python -m converter > got.lisp

cql_server:
	$(POETRY) run python -m cql_server

cql_client:
	$(POETRY) run python -m cql_client

serve-coverage:
	cd htmlcov && timeout 1h $(POETRY) run python -m http.server

parse:
	bash parse_parallel.bash

apache-cassandra-3.0.29:
	wget 'https://dlcdn.apache.org/cassandra/3.0.29/apache-cassandra-3.0.29-bin.tar.gz'
	tar xvzf apache-cassandra-3.0.29-bin.tar.gz
	rm apache-cassandra-3.0.29-bin.tar.gz

.PHONY: populate_db
populate_db: clean cass_zig
	make populate_rows
	docker stop cass_zig

	rm -rf test_data/cassandra3_data_want/
	mkdir -p test_data/cassandra3_data_want/
	cp -rp cassandra_data/data/* test_data/cassandra3_data_want/
	cp populate_test_schema.cql test_data/cassandra3_data_want/

# ====

.PHONY: cass_zig
cass_zig:
	docker run -d \
		-v $(PWD)/cassandra-3.0.yaml:/etc/cassandra/cassandra.yaml \
		-v $(PWD)/:/root/work:ro \
		-v $(PWD)/cassandra_data:/var/lib/cassandra \
		-p 9042:9042 \
		--name cass_zig cassandra:3.0 || docker start cass_zig

.PHONY: cass4_zig
cass4_zig: cassandra-4.1.4.yaml
	docker run -d \
		-v $(PWD)/cassandra-4.1.4.yaml:/etc/cassandra/cassandra.yaml \
		-v $(PWD)/:/root/work:ro \
		-v $(PWD)/cassandra_data:/var/lib/cassandra \
		-p 9042:9042 \
		--name cass4_zig cassandra:4.1.4 || docker start cass4_zig

.PHONY: cass5_zig
cass5_zig: cassandra-5.0-beta1.yaml
	docker run -d \
		-v $(PWD)/cassandra-5.0-beta1.yaml:/etc/cassandra/cassandra.yaml \
		-v $(PWD)/:/root/work:ro \
		-v $(PWD)/cassandra_data:/var/lib/cassandra \
		-p 9042:9042 \
		--name cass5_zig cassandra:5.0 || docker start cass5_zig

cassandra-5.0-beta1.yaml:
	wget -O cassandra-5.0-beta1.yaml https://raw.githubusercontent.com/apache/cassandra/cassandra-5.0-beta1/conf/cassandra.yaml

cassandra-4.1.4.yaml:
	wget -O cassandra-4.1.4.yaml https://raw.githubusercontent.com/apache/cassandra/cassandra-4.1.4/conf/cassandra.yaml

.PHONY: populate_rows
populate_rows:
	docker exec -it cass_zig /root/work/startup.bash

.PHONY: clean
clean:
	docker rm -f cass_zig cass4_zig cass5_zig
	sudo rm -rf ./cassandra_data

.PHONY: bash
bash:
	docker exec -it cass4_zig cass5_zig bash

.PHONY: consume
consume:
	docker run -v $(PWD)/:/src/ -v $(PWD)/cdc_raw/:/cdc_raw/ --rm -it groovy:latest bash
	# docker run -v $(PWD)/:/src/ --rm -it groovy:latest groovy /src/read-commitlog.groovy

