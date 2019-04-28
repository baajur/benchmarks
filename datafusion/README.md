# DataFusion Benchmarks

Executes aggregate query in parallel using a separate context per partition/thread

```bash
cargo run --release -- bench --path /home/andy/nyc-tripdata/parquet --sql "SELECT MIN(tip_amount), MAX(tip_amount) FROM tripdata" --iterations 5
cargo run --release -- bench --path /home/andy/nyc-tripdata/parquet --sql "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" --iterations 5

```


Roadmap:

- [x] Manual parallel aggregate query (hacky)
- [ ] DataFusion implementing parallel query for real
- [ ] Distributed query execution using Kubernetes
