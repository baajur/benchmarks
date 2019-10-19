# Apache Spark Benchmarks

```
root
 |-- vendor_id: string (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- pickup_longitude: double (nullable = true)
 |-- pickup_latitude: double (nullable = true)
 |-- rate_code: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- dropoff_longitude: double (nullable = true)
 |-- dropoff_latitude: double (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- surcharge: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)

```

## Convert single CSV to Parquet

Converts a single CSV file to a Parquet file.

```bash
./gradlew run --args='convert yellow_tripdata_2010-01.csv yellow_tripdata_2010-01.parquet'
```

## Run in-process benchmark

### Parquet

```bash
./gradlew run --args='bench parquet /path/to/nyc-tripdata/parquet "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" 5'
```

### CSV

```bash
./gradlew run --args='bench csv /path/to/nyc-tripdata/csv "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" 5'
```

## Rust REST API

```bash
./gradlew run --args='server'
```

# Status

- [ ] In-process
- [ ] REST single process (spark context per query)
- [ ] REST single process (shared spark context)
- [ ] Kubernetes cluster

# Queries and expected results

```sql
SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count
```

```
[192,6.0,6.0]
[1,-800.0,907070.24]
[6,-100.0,433.0]
[3,-498.0,349026.72]
[96,6.0,6.0]
[5,-300.0,1271.5]
[9,0.09,110.0]
[4,-415.0,974.5]
[8,-89.0,129.5]
[7,-70.0,140.0]
[2,-498.0,214748.44]
[0,-90.89,40502.24]
```

