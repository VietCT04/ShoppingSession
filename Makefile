CXX ?= g++
CXXFLAGS ?= -O3 -DNDEBUG -std=c++20 -pthread -Wall -Wextra -Wno-missing-field-initializers -isystem third_party/duckdb_sort
BUILD ?= build
DATA ?= data/dev
PGO_DATA ?= $(DATA)
OUT ?= out
NCPU ?= 8
EVENTS ?= 1000000
CLICKS ?= 250000
SEED ?= 1
CARGO ?= $(HOME)/.cargo/bin/cargo
NATIVE_HEADERS := $(wildcard native/*.hpp)

.PHONY: all baseline run fixtures check-dev dev benchmark clean docker test pgo
all: $(BUILD)/shopping
pgo: | $(BUILD)
	test -f "$(PGO_DATA)/manifest.json"
	set -eu; \
		training_dir=$$(mktemp -d "$(abspath $(BUILD))/.pgo.XXXXXXXX"); \
		trap 'rm -rf "$$training_dir"' EXIT; \
		$(CXX) $(CXXFLAGS) -fprofile-generate="$$training_dir/counts" -fprofile-update=atomic native/main.cpp -o "$$training_dir/shopping"; \
		"$$training_dir/shopping" "$(PGO_DATA)" "$$training_dir/output" --threads "$(NCPU)"; \
		$(CXX) $(CXXFLAGS) -fprofile-use="$$training_dir/counts" -fprofile-correction -fprofile-partial-training -Werror=missing-profile native/main.cpp -o "$$training_dir/shopping"; \
		mv "$$training_dir/shopping" "$(BUILD)/shopping-pgo"
$(BUILD)/shopping-short: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	$(CXX) $(CXXFLAGS) -DSHOP_SHORT_IDS native/main.cpp -o $@
$(BUILD)/shopping-ips: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	sh scripts/bootstrap-ips.sh
	$(CXX) $(CXXFLAGS) -DSHOP_IPS_SORTS -isystem third_party/ips4o/include -isystem third_party/ips2ra/include -isystem build/vendor/ips/root/usr/include native/main.cpp -Lbuild/vendor/ips/root/usr/lib/x86_64-linux-gnu -Wl,-rpath,'$$ORIGIN/vendor/ips/root/usr/lib/x86_64-linux-gnu' -ltbb -latomic -o $@
$(BUILD)/shopping-packed: native/main.cpp $(NATIVE_HEADERS) $(wildcard third_party/duckdb_sort/*.h*) | $(BUILD)
	$(CXX) $(CXXFLAGS) -DSHOP_WIDE_IDS native/main.cpp -o $@
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
