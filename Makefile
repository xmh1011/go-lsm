# init command params
ifeq ($(GO_1_24_BIN),)
	GO := go
else
	GO := $(GO_1_24_BIN)/go
endif

# Project Variables
BINARY_NAME = go-lsm
OUTPUT_DIR = output
TEST_FILE = unittest.txt coverage.txt bench_test.txt bench_custom.txt data
MAIN_SRC = ./main.go
GO_PACKAGES  := $(shell $(GO) list ./... | grep -vE "vendor")
GO_FILES := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
GOIMPORTS_REVISER := goimports-reviser
COMPANY_PREFIXES := "github.com/xmh1011"
IMPORTS_ORDER := "std,general,company,project"

# Commands
.PHONY: all build run clean test help prepare prepare-dep gomod set-env \
        bench benchmark benchmarks go-vet-check static-check style-check format \
        install-goimports-reviser install-staticcheck

# Default command: build the binary
all: prepare build

# set proxy env
set-env:
	$(GO) env -w GO111MODULE=on
	$(GO) env -w GONOSUMDB=*

# Download dependencies
prepare: gomod
prepare-dep:
	$(GO) env
	$(GO) mod download -x

gomod: set-env
	$(GO) mod tidy
	$(GO) mod download || $(GO) mod download -x

# Build the binary
build:
	@echo "Building the project..."
	mkdir -p $(OUTPUT_DIR)/bin
	$(GO) build -o $(OUTPUT_DIR)/bin/$(BINARY_NAME) $(MAIN_SRC)

# Run the application
run: build
	@echo "Running the application..."
	./$(OUTPUT_DIR)/bin/$(BINARY_NAME)

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
	$(GO) test -v -cover $(GO_PACKAGES) -coverpkg=./... -coverprofile=coverage.txt | tee unittest.txt

bench: clean-data prepare
	@echo "Running bench test..."
	$(GO) test -bench=. -benchtime=30s ./database | tee bench_test.txt

benchmark: clean-data prepare
	@echo "Running benchmark test..."
	$(GO) run ./benchmark/benchmark.go | tee bench_custom.txt

benchmarks: bench benchmark

# Static analysis tools
go-vet-check:
	@echo "Running go vet..."
	$(GO) vet $(GO_PACKAGES)

install-static-check:
	@echo "Installing static-check..."
	@command -v staticcheck >/dev/null 2>&1 || $(GO) install honnef.co/go/tools/cmd/staticcheck@latest

static-check: install-static-check
	@echo "Running static check..."
	staticcheck $(GO_PACKAGES)

# Code style checks
install-go-imports-reviser:
	@echo "Installing go-imports-reviser..."
	@command -v goimports-reviser >/dev/null 2>&1 || $(GO) install github.com/incu6us/goimports-reviser/v3@latest

style-check: install-go-imports-reviser
	@echo "Running style check for import package order on all Go files"
	@failed=0; \
	for file in $(GO_FILES); do \
		echo "Checking $$file"; \
		if ! $(GOIMPORTS_REVISER) -file-path "$$file" \
			-format \
			-company-prefixes $(COMPANY_PREFIXES) \
			-imports-order $(IMPORTS_ORDER) \
			-output stdout | diff -q "$$file" - >/dev/null; then \
			echo "Style check failed for: $$file"; \
			failed=1; \
		fi; \
	done; \
	if [ $$failed -eq 1 ]; then \
		echo "\nstyle check failed - files need formatting"; \
		echo "Please run 'make style-fix' to automatically fix imports"; \
		echo "ref: github.com/incu6us/goimports-reviser"; \
		exit 1; \
	else \
		echo "All files are properly formatted"; \
	fi

format: install-go-imports-reviser
	@echo "Fixing import order for all Go files"
	@$(GOIMPORTS_REVISER) \
		-format \
		-company-prefixes "$(COMPANY_PREFIXES)" \
		-imports-order "$(IMPORTS_ORDER)" \
		$(GO_FILES)

# Help message
help:
	@echo "Makefile commands:"
	@echo "  make              - Build the project (default)"
	@echo "  make build        - Build the binary in the output directory"
	@echo "  make run          - Run the application from the output directory"
	@echo "  make test         - Run tests"
	@echo "  make clean        - Remove the output directory and other generated files"
	@echo "  make help         - Show this help message"
	@echo "  make prepare      - Download dependencies"
	@echo "  make benchmarks   - Run all benchmarks"
	@echo "  make style-check  - Check code imports style"
	@echo "  make format       - Format all Go files imports"
	@echo "  make static-check - Run static analysis"
