#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_contrib;
#[macro_use] extern crate serde_derive;
extern crate arrow;
extern crate datafusion;

use std::rc::Rc;
use std::thread;
use std::time::Instant;
use std::io;
use std::fs;
use std::path::Path;

use arrow::array::{Float64Array, Int32Array, UInt32Array};
use arrow::datatypes::DataType;

use arrow::record_batch::RecordBatch;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

use rocket::State;
use rocket_contrib::json::Json;
use datafusion::datasource::MemTable;

struct QueryContext {
    partitions: Vec<String>
}

#[derive(Deserialize)]
struct Query {
    path: String,
    sql: String
}

#[post("/", data = "<query>")]
fn query(context: State<QueryContext>, query: Json<Query>) -> String {
    let partitions = visit_dirs(&Path::new(&query.path)).unwrap();
    execute_query(&partitions);
    "Hello, world!".to_string()
}

fn main() {
//    manual_test();
    run_query_server();
}

fn run_query_server() {
    let partitions = visit_dirs(&Path::new("/home/andy/nyc-tripdata/parquet")).unwrap();
    let state = QueryContext { partitions };
    rocket::ignite()
        .manage(state)
        .mount("/", routes![query])
        .launch();
}

fn manual_test() {
    let partitions = visit_dirs(&Path::new("/home/andy/nyc-tripdata/parquet")).unwrap();
    execute_query(&partitions);
}

fn execute_query(partitions: &Vec<String>) {

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
        show_batch(&batch);
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
    //println!("Query against {} took {} seconds", path, seconds);

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
