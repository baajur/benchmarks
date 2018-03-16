use std::collections::HashMap;
use std::time::Instant;

extern crate datafusion;
use datafusion::data::*;
use datafusion::dataframe::*;
use datafusion::exec::*;
use datafusion::functions::geospatial::*;
use datafusion::rel::*;

fn main() {

    // local or remote?
    let mut ctx = ExecutionContext::local("/mnt/ssd/".to_string());
//    let mut ctx =    ExecutionContext::remote("http://192.168.0.37:2379".to_string());


    generate_wkt(&mut ctx, 1000000000);

//    let mut n = 10;
//    loop {
//        generate_wkt(&mut ctx, n);
//        n = n * 10;
//    }
}

fn generate_wkt(ctx: &mut ExecutionContext, n: u32) {

    println!("Running job for {} locations", n);

    let now = Instant::now();

    // define schemas for test data
    ctx.define_schema(&format!("locations_{}", n),&Schema::new(vec![
        Field::new("id", DataType::UnsignedLong, false),
        Field::new("lat", DataType::Double, false),
        Field::new("lng", DataType::Double, false)]));

    //CREATE EXTERNAL TABLE locations (id INT, lat DOUBLE, lng DOUBLE)

    ctx.define_function(&STPointFunc {});
    ctx.define_function(&STAsText{});

    let sql = format!("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations_{}", n);

    let df = ctx.sql(&sql).unwrap();

    let filename = format!("/mnt/ssd/df_locations_wkt_{}.csv", n);

    ctx.write(df, &filename).unwrap();

    let duration = now.elapsed();
    let seconds = duration.as_secs() as f64
        + (duration.subsec_nanos() as f64 / 1000000000.0);

    println!("Elapsed time for {} locations is {} seconds ({} rows per second)",
             n, seconds, n as f64 / seconds);

}

