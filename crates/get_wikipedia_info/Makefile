help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ { printf "%-30s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

check:  ## cargo check
	cargo check

build:  ## cargo build
	cargo build

release:  ## cargo build --release
	cargo build --release

run_prod: release ## ../../target/release/get_wikipedia_info
	../../target/release/get_wikipedia_info
