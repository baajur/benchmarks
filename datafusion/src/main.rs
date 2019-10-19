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
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};

use arrow::record_batch::RecordBatch;
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
//    let partitions = visit_dirs(&Path::new(&query.path)).unwrap();
//    execute_query(&partitions, &query.sql);
    "Hello, world!".to_string()
}

fn main() {

    let matches = App::new("DataFusion Benchmarks")
        .author("Andy Grove")
        .subcommand(SubCommand::with_name("bench")
            .arg(Arg::with_name("format").long("format").takes_value(true).required(true))
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
            cmd_matches.value_of("format").unwrap(),
            cmd_matches.value_of("path").unwrap(),
            cmd_matches.value_of("sql").unwrap(),
            cmd_matches.value_of("iterations").unwrap().parse::<usize>().unwrap()
        ).unwrap(),
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

fn manual_test(format: &str, path: &str, sql: &str, iterations: usize) -> Result<()> {

    for i in 0..iterations {
        let now = Instant::now();

        let mut ctx = ExecutionContext::new();

        match format {
            "parquet" => ctx.register_parquet("tripdata", path)?,
            "csv" => {

                let schema = Schema::new(vec![
                    Field::new("VendorID", DataType::Int32, true),
                    Field::new("tpep_pickup_datetime", DataType::Timestamp(TimeUnit::Second), true),
                    Field::new("tpep_dropoff_datetime", DataType::Timestamp(TimeUnit::Second), true),
                    Field::new("passenger_count", DataType::Int32, true),
                    Field::new("trip_distance", DataType::Float64, true),
                    Field::new("RatecodeID", DataType::Int32, true),
                    Field::new("store_and_fwd_flag", DataType::Utf8, true),
                    Field::new("PULocationID", DataType::Int32, true),
                    Field::new("DOLocationID", DataType::Int32, true),
                    Field::new("payment_type", DataType::Int32, true),
                    Field::new("fare_amount", DataType::Float64, true),
                    Field::new("extra", DataType::Float64, true),
                    Field::new("mta_tax", DataType::Float64, true),
                    Field::new("tip_amount", DataType::Float64, true),
                    Field::new("tolls_amount", DataType::Float64, true),
                    Field::new("improvement_surcharge", DataType::Float64, true),
                    Field::new("total_amount", DataType::Float64, true),
                ]);

                ctx.register_csv("tripdata", path, &schema, true);
            },
            _ => panic!("Invalid format")
        }


        let batch_size = 1024 * 1024;

        let plan = ctx.create_logical_plan(sql)?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan, batch_size)?;
        let results = ctx.collect(plan.as_ref())?;
        results.iter().for_each(show_batch);

        let duration = now.elapsed();
        let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);
        println!("Iteration {} took {} seconds", i+1, seconds);
    }

    Ok(())
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
