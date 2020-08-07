use std::fs::File;
use std::rc::Rc;
use std::result::Result;
use std::thread;
use std::time::Instant;

use arrow::array::{Float64Array, StringArray};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::execution::physical_plan::common;
use parquet::arrow::arrow_reader::ArrowReader;
use parquet::arrow::ParquetFileArrowReader;
use parquet::file::reader::SerializedFileReader;
use std::collections::HashMap;

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

    for handle in worker_threads {
        let result = handle.join().unwrap();
        //TODO final agg
        let partial_agg = result.unwrap();
        for (k, v) in &partial_agg {
            println!("{:?} = {:?}", k, v);
        }
    }

    println!(
        "Read {} files in {} ms",
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
                let key = AggrKey {
                    l_returnflag: l_returnflag.value(row).to_owned(),
                    l_linestatus: l_linestatus.value(row).to_owned(),
                };
                match map.get_mut(&key) {
                    Some(accum) => do_accum(&batch, row, accum),
                    None => {
                        let mut accum = Accumulators {
                            sum_qty: 0.0,
                            sum_base_price: 0.0,
                            sum_discount: 0.0,
                            sum_disc_price: 0.0,
                            sum_charge: 0.0,
                            count_order: 0,
                        };
                        do_accum(&batch, row, &mut accum);
                        map.insert(key, accum);
                    }
                }
            }
        }
    }

    Ok(map)
}

fn do_accum(batch: &RecordBatch, row: usize, accum: &mut Accumulators) {
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

    let extended_price = l_extendedprice.value(row);
    let tax = l_tax.value(row);
    let qty = l_quantity.value(row);
    let discount_price = l_discount.value(row);
    let disc_price = extended_price * (1_f64 - discount_price);

    accum.sum_qty += qty;
    accum.sum_base_price += extended_price;
    accum.sum_discount += discount_price;
    accum.sum_disc_price += disc_price;
    accum.sum_charge += disc_price * (1_f64 + tax);
    accum.count_order += 1;
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
    count_order: usize,
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
