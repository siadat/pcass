#!/bin/bash
set -euo pipefail
set -x

(
	rm -rf capstone/
	git clone --branch=5.0.1 --depth=1 git@github.com:capstone-engine/capstone.git
	cd capstone/
	./make.sh
)
(
	rm -rf tracy/
	git clone --branch=v0.10 --depth=1 git@github.com:wolfpld/tracy.git
	cd tracy/
	make -C ./capture/build/unix/
)

LD_LIBRARY_PATH=./capstone ldd ./tracy/capture/build/unix/capture-release
LD_LIBRARY_PATH=./capstone ./tracy/capture/build/unix/capture-release || true
