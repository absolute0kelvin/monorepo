/usr/bin/time -l cargo run -p commonware-storage --bin snapshot_rss --release --  --ordered  --n 150000000  --updates-per-iter 50000  --sleep-ms 50  --commit-every 1 --storage-dir ./tmp_data
