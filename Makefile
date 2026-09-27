# Thin wrapper around src/Makefile, which is the maintained build.
#
#   make                 # Metal build: shaders, CLI, and test binaries (macOS)
#   make run             # build and run the full (GPU + CPU) test suite
#   make fast TEST=test_engine
#   make cpu-run         # portable CPU-only build + tests (Linux / CI / no Metal)
#   make clean

.DEFAULT_GOAL := all

Makefile: ;

%:
	$(MAKE) -C src $@

all:
	$(MAKE) -C src all
