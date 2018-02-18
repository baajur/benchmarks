use std::collections::HashMap;
use std::time::Instant;

extern crate datafusion;
use datafusion::dataframe::*;
use datafusion::rel::*;
use datafusion::exec::*;
use datafusion::functions::geospatial::*;

fn main() {
    sort_locations(10);
    sort_locations(100);
    sort_locations(1000);
    sort_locations(10000);
    sort_locations(100000);
    sort_locations(1000000);
    sort_locations(10000000);
    sort_locations(100000000);
}


fn sort_locations(n: u32) {

    println!("Running job for {} locations", n);

    let now = Instant::now();

    // create execution context
    let mut ctx = ExecutionContext::new("./test/data".to_string());

    // define schemas for test data
    ctx.define_schema(&format!("locations_{}", n),&Schema::new(vec![
        Field::new("id", DataType::UnsignedLong, false),
        Field::new("lat", DataType::Double, false),
        Field::new("lng", DataType::Double, false)]));


//    ctx.define_function(&STPointFunc {});
//    ctx.define_function(&STAsText{});

    let sql = format!("SELECT id, lat, lng FROM locations_{} ORDER BY lat, lng", n);

    let df = ctx.sql(&sql).unwrap();

    let filename = format!("locations_sorted_{}.csv", n);

    df.write(&filename).unwrap();
    let elapsed = now.elapsed();

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64
        + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time for {} locations is {} seconds ({} rows per second)",
             n, seconds, n as f64 / seconds);



}
