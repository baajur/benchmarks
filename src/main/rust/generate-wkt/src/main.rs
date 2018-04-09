use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

extern crate arrow;
use arrow::datatypes::*;

extern crate datafusion;
use datafusion::exec::*;
use datafusion::functions::geospatial::*;
use datafusion::logical::*;

fn main() {
    // local or remote?
    let mut ctx = ExecutionContext::local();
    //    let mut ctx =    ExecutionContext::remote("http://192.168.0.37:2379".to_string());

    let mut n = 10;
    loop {
        generate_wkt(&mut ctx, n);
        n = n * 10;
    }
}

fn generate_wkt(ctx: &mut ExecutionContext, n: u32) {
    println!("Running job for {} locations", n);

    let now = Instant::now();

    // define schemas for test data
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let filename = format!("/mnt/ssd/csv/locations_{}", n);
    let df = ctx.load_csv(&filename, &schema).unwrap();
    ctx.register("locations", df);

    ctx.register_function(Rc::new(STPointFunc {}));
    ctx.register_function(Rc::new(STAsText {}));

    let sql = format!("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations");

    let df = ctx.sql(&sql).unwrap();

    let output_filename = format!("/dev/null/df_locations_wkt_{}.csv", n);

    ctx.write_csv(df, &output_filename).unwrap();

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!(
        "Elapsed time for {} locations is {} seconds ({} rows per second)",
        n,
        seconds,
        n as f64 / seconds
    );
}
