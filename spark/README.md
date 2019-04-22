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

```bash
./gradlew run --args='bench /home/andy/nyc-tripdata/parquet "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" 5'
./gradlew run --args='bench /home/andy/nyc-tripdata/parquet "SELECT MIN(tip_amount), MAX(tip_amount) FROM tripdata" 5'
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