# DataFusion Benchmarks

## Parquet

```bash
cargo run --release -- bench --format parquet --path /path/to/nyc-tripdata/parquet --sql "SELECT MIN(tip_amount), MAX(tip_amount) FROM tripdata" --iterations 5
cargo run --release -- bench --format parquet --path /path/to/nyc-tripdata/parquet --sql "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" --iterations 5
```

## CSV

```bash
cargo run --release -- bench --format csv --path /path/to/nyc-tripdata/csv --sql "SELECT MIN(tip_amount), MAX(tip_amount) FROM tripdata" --iterations 5
cargo run --release -- bench --format csv --path /path/to/nyc-tripdata/csv --sql "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" --iterations 5
```

# Queries and expected results

```sql
SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count
```

```
["2", "-498", "214748.44"]
["3", "-498", "349026.72"]
["0", "-90.89", "40502.24"]
["1", "-800", "907070.24"]
["6", "-100", "433"]
["7", "-70", "140"]
["4", "-415", "974.5"]
["5", "-300", "1271.5"]
["192", "6", "6"]
["96", "6", "6"]
["8", "-89", "129.5"]
["9", "0.09", "110"]
```