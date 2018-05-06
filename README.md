# DataFusion Benchmarks

Benchmarks comparing the performance of DataFusion with Apache Spark.

## Disclaimer

It is hard to write fair tests at the moment since DataFusion is at such an early stage of development and only has a small subset of the functionality of Apache Spark.

This repo is badly organized and benchmarking is quite manual at the moment. I hope to clean this up soon. 

# Benchmarks

## 1. Generate WKT

This job runs the following SQL against input files of varying sizes.

```sql
SELECT ST_AsText(ST_Point(lat, lng)) FROM locations

```

This benchmark tests the following features:

- Read / Write CSV
- SQL Projection
- User Defined Functions
- User Defined Types

## 2. Simple Aggregate Queries against NYC Taxi Data

TBD

