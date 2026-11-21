CXX ?= g++
CXXFLAGS ?= -O3 -DNDEBUG -std=c++20 -pthread -Wall -Wextra -Wno-missing-field-initializers -isystem third_party/duckdb_sort
BUILD ?= build
DATA ?= data/dev
OUT ?= out
NCPU ?= 8
EVENTS ?= 1000000
CLICKS ?= 250000
SEED ?= 1
CARGO ?= $(HOME)/.cargo/bin/cargo
NATIVE_HEADERS := $(wildcard native/*.hpp)

.PHONY: all baseline run fixtures check-dev dev benchmark clean docker test
all: $(BUILD)/shopping
$(BUILD)/shopping-ips: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	sh scripts/bootstrap-ips.sh
	$(CXX) $(CXXFLAGS) -DSHOP_IPS_SORTS -isystem third_party/ips4o/include -isystem third_party/ips2ra/include -isystem build/vendor/ips/root/usr/include native/main.cpp -Lbuild/vendor/ips/root/usr/lib/x86_64-linux-gnu -Wl,-rpath,'$$ORIGIN/vendor/ips/root/usr/lib/x86_64-linux-gnu' -ltbb -latomic -o $@
$(BUILD)/shopping-packed: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	$(CXX) $(CXXFLAGS) -DSHOP_PACKED_IDS native/main.cpp -o $@
$(BUILD)/shopping-view: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	$(CXX) $(CXXFLAGS) -DSHOP_VIEW_IDS native/main.cpp -o $@
$(BUILD)/check: tools/check.cpp $(NATIVE_HEADERS) | $(BUILD)
	sh scripts/bootstrap-validator.sh
	$(CXX) $(CXXFLAGS) -Ibuild/vendor tools/check.cpp -Lbuild/vendor -lduckdb -Wl,-rpath,'$$ORIGIN/vendor' -o $@
$(BUILD):
	mkdir -p $@
$(BUILD)/shopping: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	$(CXX) $(CXXFLAGS) native/main.cpp -o $@
$(BUILD)/unit: tests/unit.cpp $(NATIVE_HEADERS) | $(BUILD)
	$(CXX) $(CXXFLAGS) tests/unit.cpp -o $@
test: $(BUILD)/unit fixtures
	$(BUILD)/unit
baseline: all
	$(BUILD)/shopping $(DATA) $(OUT)-baseline --baseline
run: all
	$(BUILD)/shopping $(DATA) $(OUT) --threads $(NCPU)
fixtures: all $(BUILD)/check
	$(BUILD)/check fixtures
check-dev: $(BUILD)/check
	$(BUILD)/check compare $(OUT) data/dev-answer
benchmark: all $(BUILD)/check
	$(BUILD)/check bench $(DATA) data/dev-answer 3
dev:
	$(CARGO) build --release --manifest-path tools/generator/Cargo.toml
	tools/generator/target/release/generate --seed $(SEED) --events $(EVENTS) --clicks $(CLICKS) --threads $(NCPU) --output $(DATA)
clean:
	rm -rf $(BUILD)
docker:
	docker build --platform linux/amd64 -t shopping-sessions:latest .
