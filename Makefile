# Compile the executable (Rustc and cargo required!)
compile:
	@echo "Compiling Rust project..."
	cargo build --release
	mv target/release/ebi-to-gcp ebi-to-gcp
	cargo clean