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

fn main() {
    let path = "/mnt/ssd/nyc_taxis/parquet/yellow_tripdata_2017-12/part-00000-429f3db9-7d51-4636-87f0-944fee7d304c-c000.snappy.parquet";
//    let path = "/mnt/ssd/nyc_taxis/yellow_tripdata_2017-12.csv";
    match File::open(path) {
        Ok(_) => {

            let now = Instant::now();

            // create execution context
            let mut ctx = ExecutionContext::local();

            let field_names = vec![
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
                "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                "improvement_surcharge", "total_amount"];

            let fields: Vec<Field> = field_names.iter()
                .map(|name| Field::new(name, DataType::Utf8, false)).collect();

            let schema = Schema::new(fields);

            // open a CSV file as a dataframe
            let tripdata = ctx.load_parquet(path, ).unwrap();
//            let tripdata = ctx.load_csv(path, &schema, true, None).unwrap();

            // register as a table so we can run SQL against it
            ctx.register("tripdata", tripdata);

            // define the SQL statement
//            let sql = "SELECT passenger_count, COUNT(1) FROM tripdata GROUP BY passenger_count";
//            let sql = "SELECT COUNT(passenger_count) FROM tripdata";

            let sql = "SELECT passenger_count, \
                COUNT(1), \
                MIN(CAST(fare_amount AS FLOAT)), \
                MAX(CAST(fare_amount AS FLOAT)) \
            FROM tripdata \
            GROUP BY passenger_count";


            let sql = "SELECT passenger_count, \
                COUNT(1), \
                MIN(fare_amount), \
                MAX(fare_amount) \
            FROM tripdata \
            GROUP BY passenger_count";

            //

            // create a data frame
            let df = ctx.sql(&sql).unwrap();

            ctx.write_csv(df, "_results.csv").unwrap();

            let duration = now.elapsed();
            let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

            println!("Elapsed time is {} seconds", seconds);

        }
        _ => println!("Could not locate {} - try downloading it from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml", path)
    }
}
