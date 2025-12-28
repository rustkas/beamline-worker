.PHONY: fmt clippy test run

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy -- -D warnings

test:
	cargo test -- --test-threads=1

run:
	cargo run
