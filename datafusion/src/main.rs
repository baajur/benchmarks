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
use std::io;
use std::fs;
use std::path::Path;

extern crate arrow;
extern crate datafusion;

use arrow::array::{Float64Array, Int32Array, UInt32Array};
use arrow::datatypes::DataType;

use arrow::record_batch::RecordBatch;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

fn main() {

    let partitions = visit_dirs(&Path::new("/home/andy/nyc-tripdata/parquet")).unwrap();

    for p in &partitions {
        println!("{}", p);
    }

    let sql = "SELECT passenger_count, \
               MIN(fare_amount), \
               MAX(fare_amount)\
               FROM tripdata \
               GROUP BY passenger_count";

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
        //show_batch(&batch);/year=2011
    }

    //TODO aggregate the results

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Query took {} seconds", seconds);
}

fn visit_dirs(dir: &Path) -> io::Result<Vec<String>> {
    let mut files: Vec<String> = vec![];
    if dir.is_dir() {
        let entries = fs::read_dir(dir)?;
        let mut success = false;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let path = entry.path().into_os_string().into_string().unwrap();
            if path.ends_with("_SUCCESS") {
                success = true;
                break;
            }
        }

        let entries = fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let mut dir_files = visit_dirs(&path)?;
                files.append(&mut dir_files);
            } else {
                if success {
                    let path = entry.path().into_os_string().into_string().unwrap();
                    if path.ends_with(".parquet") {
                        files.push(path);
                    }
                }
            }
        }
    }
    Ok(files)
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
