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

use clap::{Arg, App, SubCommand};

struct QueryContext {
}

#[derive(Deserialize)]
struct Query {
    path: String,
    sql: String
}

#[post("/", data = "<query>")]
fn query(_context: State<QueryContext>, query: Json<Query>) -> String {
    let partitions = visit_dirs(&Path::new(&query.path)).unwrap();
    execute_query(&partitions, &query.sql);
    "Hello, world!".to_string()
}

fn main() {

    let matches = App::new("DataFusion Benchmarks")
        .author("Andy Grove")
        .subcommand(SubCommand::with_name("bench")
            .arg(Arg::with_name("sql").long("sql").takes_value(true).required(true))
            .arg(Arg::with_name("path").long("path").takes_value(true).required(true))
            .arg(Arg::with_name("iterations").long("iterations").takes_value(true).required(true))
        )
        .subcommand(SubCommand::with_name("server"))
        .get_matches();


    let (cmd, cmd_matches) = matches.subcommand();
    let cmd_matches = cmd_matches.unwrap();

    match cmd {
        "bench" => manual_test(
            cmd_matches.value_of("path").unwrap(),
            cmd_matches.value_of("sql").unwrap(),
            cmd_matches.value_of("iterations").unwrap().parse::<usize>().unwrap()
        ),
        "server" => run_query_server(),
        _ => println!("???")
    }
}

fn run_query_server() {
    let state = QueryContext {};
    rocket::ignite()
        .manage(state)
        .mount("/", routes![query])
        .launch();
}

fn manual_test(path: &str, sql: &str, iterations: usize) {
    for i in 0..iterations {
        let now = Instant::now();
        let partitions = visit_dirs(&Path::new(path)).unwrap();
        println!("Found {} partitions", partitions.len());
        execute_query(&partitions, sql);
        let duration = now.elapsed();
        let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);
        println!("Iteration {} took {} seconds", i+1, seconds);
    }
}

fn execute_query(partitions: &Vec<String>, sql: &str) {


    let mut handles = vec![];
    for path in partitions {
        let path = path.to_string();
        let sql = sql.to_string();
        handles.push(thread::spawn(move || {
            execute_aggregate(&path, &sql).unwrap().unwrap()
        }));
    }

    // wait for all threads to finish
    for handle in handles {
        let batch = handle.join().unwrap();
        show_batch(&batch);
    }

    //TODO aggregate the results

}

fn visit_dirs(dir: &Path) -> io::Result<Vec<String>> {
    let mut files: Vec<String> = vec![];
    if dir.is_dir() {
        let entries = fs::read_dir(dir)?;
        let mut success = false;
        for entry in entries {
            let entry = entry?;
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

    // create execution context
    let mut ctx = ExecutionContext::new();

    ctx.register_table("tripdata", Rc::new(ParquetTable::try_new(path).unwrap()));

    // create a data frame
    let result = ctx.sql(&sql, 1024).unwrap();

    let mut ref_mut = result.borrow_mut();
    let batch = ref_mut.next();

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
