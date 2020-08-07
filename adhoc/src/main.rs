use std::fs::File;
use std::rc::Rc;
use std::result::Result;
use std::thread;
use std::time::Instant;

use arrow::array::{Float64Array, StringArray};
use arrow::record_batch::RecordBatchReader;
use datafusion::execution::physical_plan::common;
use parquet::arrow::arrow_reader::ArrowReader;
use parquet::arrow::ParquetFileArrowReader;
use parquet::file::reader::SerializedFileReader;
use std::collections::HashMap;

/// TPCH Query 1.
///
/// The full SQL is:
///
/// select
///     l_returnflag,
///     l_linestatus,
///     sum(l_quantity) as sum_qty,
///     sum(l_extendedprice) as sum_base_price,
///     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
///     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
///     avg(l_quantity) as avg_qty,
///     avg(l_extendedprice) as avg_price,
///     avg(l_discount) as avg_disc,
///     count(*) as count_order
/// from
///     lineitem
/// where
///     l_shipdate <= date '1998-12-01' - interval ':1' day (3)
/// group by
///     l_returnflag,
///     l_linestatus
/// order by
///     l_returnflag,
///     l_linestatus;
///
#[tokio::main]
async fn main() -> Result<(), String> {
    let start = Instant::now();

    let path = "/mnt/tpch/parquet/100-240/lineitem/";
    let mut filenames: Vec<String> = vec![];
    common::build_file_list(path, &mut filenames, ".parquet").unwrap();

    let thread_count = 48;
    let chunks = filenames.chunks(filenames.len() / thread_count);

    println!("Launching {} threads", chunks.len());

    let mut worker_threads = vec![];
    for chunk in chunks {
        let filenames = chunk.to_vec();
        worker_threads.push(thread::spawn(move || partial_agg(&filenames)));
    }

    // collect the partial aggregates and combine them to get the final aggregate result
    let mut final_agg: HashMap<AggrKey, Accumulators> = HashMap::new();
    for handle in worker_threads {
        let result = handle.join().unwrap();
        let partial_agg = result.unwrap();
        for (k, v) in &partial_agg {
            match final_agg.get_mut(k) {
                Some(accum) => do_final_accum(accum, v),
                None => {
                    let mut accum = Accumulators::new();
                    do_final_accum(&mut accum, v);
                    final_agg.insert(k.clone(), accum);
                }
            }
        }
    }

    //TODO sort the final results

    println!("l_returnflag, l_linestatus, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order");
    for (k, v) in &final_agg {
        println!(
            "{}, {}, {}, {}, {}, {}, {}, {}, {}",
            k.l_returnflag,
            k.l_returnflag,
            v.sum_base_price,
            v.sum_disc_price,
            v.sum_charge,
            v.sum_qty / v.count_order as f64,
            v.sum_discount / v.count_order as f64,
            v.sum_qty / v.count_order as f64,
            v.count_order,
        );
    }

    println!(
        "Processed {} files in {} ms",
        filenames.len(),
        start.elapsed().as_millis()
    );
    Ok(())
}

fn partial_agg(filenames: &Vec<String>) -> Result<HashMap<AggrKey, Accumulators>, String> {
    let batch_size = 64 * 1024;
    let parquet_buf_reader_size = 1024 * 1024;
    let projection: Vec<usize> = vec![4, 5, 6, 7, 8, 9];

    let mut map: HashMap<AggrKey, Accumulators> = HashMap::new();

    for filename in filenames {
        let file = File::open(filename).unwrap();
        let file_reader =
            Rc::new(SerializedFileReader::with_capacity(file, parquet_buf_reader_size).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let mut batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.clone(), batch_size)
            .unwrap();

        while let Some(batch) = batch_reader.next_batch().unwrap() {
            let l_quantity = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("l_quantity");
            let l_extendedprice = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("l_extendedprice");
            let l_discount = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("l_discount");
            let l_tax = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("l_tax");
            let l_returnflag = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("l_return_flag");
            let l_linestatus = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("l_linestatus");

            for row in 0..batch.num_rows() {
                //TODO filter l_shipdate <= date '1998-12-01' - interval ':1' day (3)

                let key = AggrKey {
                    l_returnflag: l_returnflag.value(row).to_owned(),
                    l_linestatus: l_linestatus.value(row).to_owned(),
                };

                let extended_price = l_extendedprice.value(row);
                let discount = l_discount.value(row);
                let tax = l_tax.value(row);
                let qty = l_quantity.value(row);

                match map.get_mut(&key) {
                    Some(accum) => do_partial_accum(extended_price, discount, tax, qty, accum),
                    None => {
                        let mut accum = Accumulators::new();
                        do_partial_accum(extended_price, discount, tax, qty, &mut accum);
                        map.insert(key, accum);
                    }
                }
            }
        }
    }

    Ok(map)
}

fn do_partial_accum(
    extended_price: f64,
    discount: f64,
    tax: f64,
    qty: f64,
    accum: &mut Accumulators,
) {
    let disc_price = extended_price * (1_f64 - discount);
    accum.sum_qty += qty;
    accum.sum_tax += tax;
    accum.sum_base_price += extended_price;
    accum.sum_discount += discount;
    accum.sum_disc_price += disc_price;
    accum.sum_charge += disc_price * (1_f64 + tax);
    accum.count_order += 1;
}

fn do_final_accum(accum_final: &mut Accumulators, accum_partial: &Accumulators) {
    accum_final.sum_qty += accum_partial.sum_qty;
    accum_final.sum_tax += accum_partial.sum_tax;
    accum_final.sum_base_price += accum_partial.sum_base_price;
    accum_final.sum_discount += accum_partial.sum_discount;
    accum_final.sum_disc_price += accum_partial.sum_disc_price;
    accum_final.sum_charge += accum_partial.sum_charge;
    accum_final.count_order += accum_partial.count_order;
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct AggrKey {
    l_returnflag: String,
    l_linestatus: String,
}

#[derive(Debug)]
struct Accumulators {
    sum_qty: f64,
    sum_base_price: f64,
    sum_discount: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_tax: f64,
    count_order: usize,
}

impl Accumulators {
    fn new() -> Self {
        Self {
            sum_qty: 0.0,
            sum_base_price: 0.0,
            sum_discount: 0.0,
            sum_disc_price: 0.0,
            sum_charge: 0.0,
            sum_tax: 0.0,
            count_order: 0,
        }
    }
}

/*
Schema { fields: [
0                Field { name: "l_orderkey", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false },
1                Field { name: "l_partkey", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false },
2                Field { name: "l_suppkey", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false },
3                Field { name: "l_linenumber", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false },
4                Field { name: "l_quantity", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false },
5                Field { name: "l_extendedprice", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false },
6                Field { name: "l_discount", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false },
7                Field { name: "l_tax", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false },
8                Field { name: "l_returnflag", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
9                Field { name: "l_linestatus", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
10                Field { name: "l_shipdate", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
11                Field { name: "l_commitdate", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
12                Field { name: "l_receiptdate", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
13                Field { name: "l_shipinstruct", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
14                Field { name: "l_shipmode", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false },
15                Field { name: "l_comment", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false }],
 */
