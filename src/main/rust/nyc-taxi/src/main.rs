// Copyright 2018 Grove Enterprises LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::time::Instant;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::exec::*;

/// Uses data from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
fn main() {
    test_parquet()
//    test_csv_untyped()
}

fn test_parquet() {
    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::local();

    load_parquet(&mut ctx);

//    let sql = "SELECT  \
//        COUNT(1), \
//        MIN(fare_amount), \
//        MAX(fare_amount) \
//    FROM tripdata \
//    ";

    let sql = "SELECT passenger_count, \
        COUNT(1), \
        MIN(fare_amount), \
        MAX(fare_amount)\
    FROM tripdata \
    GROUP BY passenger_count";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();

    df.show(1000);

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time is {} seconds", seconds);

}

fn test_csv_untyped() {
    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::local();

    load_csv_untyped(&mut ctx);


    // define the SQL statement
//            let sql = "SELECT passenger_count, COUNT(1) FROM tripdata GROUP BY passenger_count";
//            let sql = "SELECT COUNT(passenger_count) FROM tripdata";

//            let sql = "SELECT passenger_count, \
//                COUNT(1), \
//                MIN(CAST(fare_amount AS FLOAT)), \
//                MAX(CAST(fare_amount AS FLOAT)) \
//            FROM tripdata \
//            GROUP BY passenger_count";


    //[9508276,-340.0,391911.78]
//    let sql = "SELECT  \
//        COUNT(1), \
//        MIN(CAST(fare_amount AS FLOAT)), \
//        MAX(CAST(fare_amount AS FLOAT)) \
//    FROM tripdata \
//    ";

    let sql = "SELECT passenger_count, \
        COUNT(1), \
        MIN(CAST(fare_amount AS FLOAT)), \
        MAX(CAST(fare_amount AS FLOAT)) \
    FROM tripdata \
    GROUP BY passenger_count";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();

    df.show(1000);

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time is {} seconds", seconds);
}

fn test_csv_typed() {
    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::local();

    load_csv_typed(&mut ctx);


    // define the SQL statement
//            let sql = "SELECT passenger_count, COUNT(1) FROM tripdata GROUP BY passenger_count";
//            let sql = "SELECT COUNT(passenger_count) FROM tripdata";

//            let sql = "SELECT passenger_count, \
//                COUNT(1), \
//                MIN(CAST(fare_amount AS FLOAT)), \
//                MAX(CAST(fare_amount AS FLOAT)) \
//            FROM tripdata \
//            GROUP BY passenger_count";


    //[9508276,-340.0,391911.78]
    let sql = "SELECT  \
        COUNT(1), \
        MIN(fare_amount), \
        MAX(fare_amount) \
    FROM tripdata \
    ";

//    let sql = "SELECT passenger_count, \
//        COUNT(1), \
//        MIN(fare_amount), \
//        MAX(fare_amount)\
//    FROM tripdata \
//    GROUP BY passenger_count";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();

    df.show(1000);

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time is {} seconds", seconds);
}

fn load_csv_typed(ctx: &mut ExecutionContext) {

    let path = "/mnt/ssd/nyc_taxis/yellow_tripdata_2017-12.csv";

    let fields = vec![
        Field::new("VendorID", DataType::Utf8, false),
        Field::new("tpep_pickup_datetime", DataType::Utf8, false),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, false),
        Field::new("passenger_count", DataType::UInt8, false),
        Field::new("trip_distance", DataType::Utf8, false),
        Field::new("RatecodeID", DataType::Utf8, false),
        Field::new("store_and_fwd_flag", DataType::Utf8, false),
        Field::new("PULocationID", DataType::Utf8, false),
        Field::new("DOLocationID", DataType::Utf8, false),
        Field::new("payment_type", DataType::Utf8, false),
        Field::new("fare_amount", DataType::Float64, false),
        Field::new("extra", DataType::Utf8, false),
        Field::new("mta_tax", DataType::Utf8, false),
        Field::new("tip_amount", DataType::Utf8, false),
        Field::new("tolls_amount", DataType::Utf8, false),
        Field::new("improvement_surcharge", DataType::Utf8, false),
        Field::new("total_amount", DataType::Utf8, false),
    ];

    let schema = Schema::new(fields);

    // open a CSV file as a dataframe
    let tripdata = ctx.load_csv(path, &schema, true, None).unwrap();
    ctx.register("tripdata", tripdata);

}

fn load_csv_untyped(ctx: &mut ExecutionContext) {

    let path = "/mnt/ssd/nyc_taxis/yellow_tripdata_2017-12.csv";

    let fields = vec![
        Field::new("VendorID", DataType::Utf8, false),
        Field::new("tpep_pickup_datetime", DataType::Utf8, false),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, false),
        Field::new("passenger_count", DataType::UInt8, false),
        Field::new("trip_distance", DataType::Utf8, false),
        Field::new("RatecodeID", DataType::Utf8, false),
        Field::new("store_and_fwd_flag", DataType::Utf8, false),
        Field::new("PULocationID", DataType::Utf8, false),
        Field::new("DOLocationID", DataType::Utf8, false),
        Field::new("payment_type", DataType::Utf8, false),
        Field::new("fare_amount", DataType::Utf8, false),
        Field::new("extra", DataType::Utf8, false),
        Field::new("mta_tax", DataType::Utf8, false),
        Field::new("tip_amount", DataType::Utf8, false),
        Field::new("tolls_amount", DataType::Utf8, false),
        Field::new("improvement_surcharge", DataType::Utf8, false),
        Field::new("total_amount", DataType::Utf8, false),
    ];

    let schema = Schema::new(fields);

    // open a CSV file as a dataframe
    let tripdata = ctx.load_csv(path, &schema, true, None).unwrap();
    ctx.register("tripdata", tripdata);

}

/// Load a parquet version of the data that was created using Apache Spark
fn load_parquet(ctx: &mut ExecutionContext) {
    let path = "/mnt/ssd/nyc_taxis/parquet/yellow_tripdata_2017-12/part-00000-b194f35d-4f5b-4ca9-9a51-322fb8616bc1-c000.snappy.parquet";
    let tripdata = ctx.load_parquet(path,None).unwrap();
    ctx.register("tripdata", tripdata);

}

