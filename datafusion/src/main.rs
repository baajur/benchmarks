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
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::datasource::CsvDataSource;
use datafusion::execution::relation::Relation;

/// Uses data from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
fn main() {
//    let now = Instant::now();
//
//    // create execution context
//    let mut ctx = ExecutionContext::new();
//
//    load_parquet(&mut ctx);
//
////    let sql = "SELECT  \
////        COUNT(1), \
////        MIN(fare_amount), \
////        MAX(fare_amount) \
////    FROM tripdata \
////    ";
//
//    let sql = "SELECT passenger_count, \
//        COUNT(1), \
//        MIN(fare_amount), \
//        MAX(fare_amount)\
//    FROM tripdata \
//    GROUP BY passenger_count";
//
//    // create a data frame
//    let df = ctx.sql(&sql).unwrap();
//
//    //df.show(1000);
//
//    let duration = now.elapsed();
//    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);
//
//    println!("Elapsed time is {} seconds", seconds);
//
//}


fn show(df: Rc<RefCell<Relation>>) {

    let mut results = df.borrow_mut();

    while let Some(batch) = results.next().unwrap() {
        println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }

}

