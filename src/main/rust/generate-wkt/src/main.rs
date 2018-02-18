use std::collections::HashMap;
use std::time::Instant;

extern crate datafusion;
use datafusion::dataframe::*;
use datafusion::rel::*;
use datafusion::exec::*;
use datafusion::functions::geospatial::*;

fn main() {
    generate_wkt(10);
    generate_wkt(100);
    generate_wkt(1000);
    generate_wkt(10000);
    generate_wkt(100000);
    generate_wkt(1000000);
    generate_wkt(10000000);
    generate_wkt(100000000);


//    run_job_2(1000000000);
}

fn generate_wkt(n: u32) {

    println!("Running job for {} locations", n);

    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::new("/tmp".to_string());

    // define schemas for test data
    ctx.define_schema(&format!("locations_{}", n),&Schema::new(vec![
        Field::new("id", DataType::UnsignedLong, false),
        Field::new("lat", DataType::Double, false),
        Field::new("lng", DataType::Double, false)]));


    ctx.define_function(&STPointFunc {});
    ctx.define_function(&STAsText{});

    let sql = format!("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations_{}", n);

    let df = ctx.sql(&sql).unwrap();

    let filename = format!("locations_wkt_{}.csv", n);

    df.write(&filename).unwrap();
    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64
        + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time for {} locations is {} seconds ({} rows per second)",
             n, seconds, n as f64 / seconds);




}

