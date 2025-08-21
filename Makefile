# paths
INCLUDE_DIR := include
SHADER_SRC  := shaders/scan.metal
SHADER_AIR  := build/scan.air
SHADER_LIB  := build/scan.metallib
HOST_SRC    := src/main.cpp
HOST_BIN    := build/metaldb

# toolchain
METAL      := xcrun -sdk macosx metal
METALLIB   := xcrun -sdk macosx metallib
CXX        := clang++
CXXFLAGS   := -std=c++17 -Wall -Wextra -I$(INCLUDE_DIR) -I$(INCLUDE_DIR)/metal-cpp
LDFLAGS    := -framework Metal -framework Foundation

.PHONY: all clean

all: $(SHADER_LIB) $(HOST_BIN)

$(SHADER_AIR): $(SHADER_SRC) | build
	$(METAL) -c $< -o $@

$(SHADER_LIB): $(SHADER_AIR)
	$(METALLIB) $< -o $@

$(HOST_BIN): $(HOST_SRC) | build
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

build:
	mkdir -p build

clean:
	rm -rf build

