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

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

extern crate arrow;
extern crate datafusion;

use arrow::array::{UInt32Array, Int32Array, Float64Array};
use arrow::datatypes::DataType;

use datafusion::datasource::parquet::ParquetTable;
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

    /*
    RecordBatch has 8 rows and 3 columns
["2", "2.5", "200"]
["0", "2.5", "45"]
["113", "13.3", "13.3"]
["6", "2.5", "106.9"]
["4", "2.5", "187.7"]
["3", "2.5", "190.1"]
["1", "2.5", "200"]
["5", "2.5", "179.3"]
Elapsed time is 1.995201016 seconds
*/

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

}

