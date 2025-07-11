# init command params
ifeq ($(GO_1_24_BIN),)
	GO := go
else
	GO := $(GO_1_24_BIN)/go
endif

# Project Variables
BINARY_NAME = go-lsm
OUTPUT_DIR = output
TEST_FILE = unittest.txt coverage.out bench_test.txt bench_custom.txt data
MAIN_SRC = ./main.go
GO_PACKAGES  := $$($(GO) list ./...| grep -vE "vendor")

# Commands
.PHONY: all build run clean test help prepare prepare-dep gomod set-env

# Default command: build the binary
all: prepare build
# set proxy env
set-env:
	$(GO) env -w GO111MODULE=on
	$(GO) env -w GONOSUMDB=\*

#make prepare, download dependencies
prepare: gomod
prepare-dep:
	$(GO) env
	$(GO) mod download -x
gomod: set-env
	$(GO) mod tidy
	$(GO) mod download || $(GO) mod download -x  # 下载 依赖

# Build the binary
build:
	@echo "Building the project..."
	mkdir -p $(OUTPUT_DIR)
	$(GO) build -o $(OUTPUT_DIR)/bin/$(BINARY_NAME) $(MAIN_SRC)

# Run the application
run: build
	@echo "Running the application..."
	./$(OUTPUT_DIR)/$(BINARY_NAME)

# Clean up binary and temporary files
clean:
	@echo "Cleaning up..."
	rm -rf $(OUTPUT_DIR)
	rm -rf $(TEST_FILE)
	find . -type f \( -name "*.sst" -o -name "*.wal" \) -delete
	$(GO) clean

clean-data:
	@echo "Cleaning up data files..."
	find . -type f \( -name "*.sst" -o -name "*.wal" \) -delete

# Test the application
test: prepare test-case
test-case:
	@echo "Running tests..."
	$(GO) test -v -cover $(GO_PACKAGES) -coverpkg=./... -coverprofile=coverage.out | tee unittest.txt

bench: clean-data prepare
	@echo "Running bench test..."
	$(GO) test -bench=. -benchtime=30s ./database | tee bench_test.txt

benchmark: clean-data prepare
	@echo "Running benchmark test..."
	$(GO) run ./benchmark/benchmark.go | tee bench_custom.txt

benchmarks: bench benchmark

# Help message
help:
	@echo "Makefile commands:"
	@echo "  make             - Build the project (default)"
	@echo "  make build       - Build the binary in the output directory"
	@echo "  make run         - Run the application from the output directory"
	@echo "  make test        - Run tests"
	@echo "  make clean       - Remove the output directory and other generated files"
	@echo "  make help        - Show this help message"
	@echo "  make prepare     - Download dependencies"
	@echo "  make benchmarks  - Benchmark tests"
