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

use std::rc::Rc;
use std::thread;
use std::time::Instant;

extern crate arrow;
extern crate datafusion;

use arrow::array::{Float64Array, Int32Array, UInt32Array};
use arrow::datatypes::DataType;

use arrow::record_batch::RecordBatch;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

fn main() {
    let partitions = vec![
        "/home/andy/nyc-tripdata/parquet/year=2009/month=07/part-00000-91f987b1-e61a-4d8d-830d-9886bf99d957-c000.snappy.parquet",
        "/home/andy/nyc-tripdata/parquet/year=2009/month=01/part-00000-5027c231-e158-4301-b7f5-6ff371c28df9-c000.snappy.parquet",
        "/home/andy/nyc-tripdata/parquet/year=2009/month=02/part-00000-fb9bd700-47f9-4986-b5c0-dccc7a55448c-c000.snappy.parquet",
        "/home/andy/nyc-tripdata/parquet/year=2009/month=04/part-00000-972cb6cf-e108-40d1-b917-0d0257f58e48-c000.snappy.parquet",
        "/home/andy/nyc-tripdata/parquet/year=2009/month=03/part-00000-c8d1adc1-96de-46f0-ad6f-870456173a19-c000.snappy.parquet",
        "/home/andy/nyc-tripdata/parquet/year=2009/month=06/part-00000-d46dc8f6-8e49-4b7c-9f2b-3d73d6394385-c000.snappy.parquet",
        "/home/andy/nyc-tripdata/parquet/year=2009/month=05/part-00000-670cb9a0-53e6-4c7d-985c-78c50f555276-c000.snappy.parquet",
    ];

    let sql = "SELECT Passenger_Count, \
               MIN(Fare_Amt), \
               MAX(Fare_Amt)\
               FROM tripdata \
               GROUP BY Passenger_Count";

    let now = Instant::now();

    let mut handles = vec![];
    for path in partitions {
        let path = path.to_string();
        handles.push(thread::spawn(move || {
            execute_aggregate(&path, sql).unwrap().unwrap()
        }));
    }

    // wait for all threads to finish
    for handle in handles {
        let batch = handle.join().unwrap();
        show_batch(&batch);
    }

    //TODO aggregate the results

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Query took {} seconds", seconds);
}

/// Execute an aggregate query on a single parquet file
fn execute_aggregate(path: &str, sql: &str) -> Result<Option<RecordBatch>> {
    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::new();

    ctx.register_table("tripdata", Rc::new(ParquetTable::try_new(path).unwrap()));

    // create a data frame
    let result = ctx.sql(&sql, 1024).unwrap();

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    let mut ref_mut = result.borrow_mut();
    let batch = ref_mut.next();
    println!("Query against {} took {} seconds", path, seconds);

    batch
}

fn show_batch(batch: &RecordBatch) {
    for row in 0..batch.num_rows() {
        let mut line = Vec::with_capacity(batch.num_columns());
        for col in 0..batch.num_columns() {
            let array = batch.column(col);
            match array.data_type() {
                DataType::Int32 => {
                    let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    line.push(format!("{}", array.value(row)));
                }
                DataType::UInt32 => {
                    let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    line.push(format!("{}", array.value(row)));
                }
                DataType::Float64 => {
                    let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    line.push(format!("{}", array.value(row)));
                }
                other => {
                    line.push(format!("unsupported type {:?}", other));
                }
            }
        }
        println!("{:?}", line);
    }
}
