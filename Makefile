# Compile the executable (Rustc and cargo required!)
# NOTE! The compilation is done for the x86_64-unknown-linux-gnu target.
compile:
	@echo "Compiling Rust project..."
	@cargo build --release --target x86_64-unknown-linux-gnu
	@mv target/x86_64-unknown-linux-gnu/release/ebi-to-gcp ebi-to-gcp 
	@cargo clean