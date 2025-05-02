cargo build --release
echo 3 | sudo tee /proc/sys/vm/drop_caches
# time ./target/release/naive_db 