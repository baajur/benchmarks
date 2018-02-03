use std::collections::HashMap;
use std::time::Instant;

extern crate datafusion;
use datafusion::dataframe::*;
use datafusion::rel::*;
use datafusion::exec::*;
use datafusion::functions::geospatial::*;

fn main() {
    run_job(10);
    run_job(100);
    run_job(1000);
    run_job(10000);
    run_job(100000);
    run_job(1000000);
    run_job(10000000);
    run_job(100000000);
    run_job(1000000000);
}

fn run_job(n: u32) {

    println!("Running job for {} locations", n);

    let now = Instant::now();

    // create a schema registry
    let mut schemas : HashMap<String, Schema> = HashMap::new();

    // define schemas for test data
    schemas.insert(format!("locations_{}", n), Schema::new(vec![
        Field::new("id", DataType::UnsignedLong, false),
        Field::new("lat", DataType::Double, false),
        Field::new("lng", DataType::Double, false)]));

    // create execution context
    let mut ctx = ExecutionContext::new(schemas.clone());

    ctx.define_function(&STPointFunc {});
    ctx.define_function(&STAsText{});

    let sql = format!("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations_{}", n);

    let df = ctx.sql(&sql).unwrap();

    let filename = format!("locations_wkt_{}.csv", n);

    df.write(&filename).unwrap();

    println!("Elapsed time for {} locations is {} seconds", n, now.elapsed().as_secs());




}
