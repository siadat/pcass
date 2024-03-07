#!/bin/bash
set -euo pipefail
set -x
brew list emscripten > /dev/null || brew install emscripten

(
	# build capstone
	rm -rf capstone/
	# TODO: try 5.0.1
	git clone --branch=4.0.2 --depth=1 git@github.com:capstone-engine/capstone.git
	cd capstone/

	# The default emscripten cache dir is $HOMEBREW_PREFIX/Cellar/emscripten/3.1.55/libexec/cache,
	# but Tracy expects it to be $HOME/.emscripten_cache, so we are asking emscripten to use that
	# cache dir by setting the EM_CACHE environment variable.
	# Source: https://emscripten.org/docs/tools_reference/emcc.html#:~:text=overridden%20using%20the-,EM_CACHE,-environment%20variable%20or
	#   > The Emscripten cache defaults to emscripten/cache
	#   > but can be overridden using the EM_CACHE environment
	#   > variable or CACHE config setting.
	export EM_CACHE=$HOME/.emscripten_cache
	rm -rf $HOME/.emscripten_cache

	emcmake cmake -B build/
	cmake --build build/ --target install
        find $HOME/.emscripten_cache | grep capstone
)

(
	# build tracy
	rm -rf tracy/
	git clone --branch=v0.10 --depth=1 git@github.com:wolfpld/tracy.git
	cd tracy/

	cd profiler/build/wasm/
	make clean
	make debug
)
