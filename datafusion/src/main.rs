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
use datafusion::datasource::parquet::{ParquetFile, ParquetTable, };
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::relation::Relation;

fn main() {
    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::new();

    let filename = "/home/andy/nyc-tripdata/parquet/yellow_tripdata_2009-01.parquet/part-00000-156e4a16-37be-44dc-b4e8-5155e08ce7d3-c000.snappy.parquet";

    ctx.register_table("tripdata", Rc::new(ParquetTable::try_new(filename).unwrap()));

    let sql = "SELECT Passenger_Count, \
        MIN(Fare_Amt), \
        MAX(Fare_Amt)\
    FROM tripdata \
    GROUP BY Passenger_Count";

    // create a data frame
    let result = ctx.sql(&sql, 1024).unwrap();

    show(result);

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time is {} seconds", seconds);

}


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

