use std::fs::File;
use std::rc::Rc;
use std::result::Result;
use std::time::Instant;

use arrow::record_batch::RecordBatchReader;
use datafusion::execution::physical_plan::common;
use parquet::arrow::ParquetFileArrowReader;
use parquet::arrow::arrow_reader::ArrowReader;
use parquet::file::reader::SerializedFileReader;

#[tokio::main]
async fn main() -> Result<(), String> {

    let start = Instant::now();

    let path = "/mnt/tpch/parquet/100-240/lineitem/";
    let mut filenames: Vec<String> = vec![];
    common::build_file_list(path, &mut filenames, ".parquet").unwrap();

    let projection: Vec<usize> = vec![0,1];
    let batch_size = 64 * 1024;
    let parquet_buf_reader_size = 1024*1024;

    let mut rows = 0;
    for filename in &filenames {
        println!("Reading {}", filename);
        let file = File::open(filename).unwrap();
        let file_reader = Rc::new(SerializedFileReader::with_capacity(file, parquet_buf_reader_size).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let mut batch_reader = arrow_reader.get_record_reader_by_columns(projection.clone(), batch_size).unwrap();
        while let Some(batch) = batch_reader.next_batch().unwrap() {
            rows += batch.num_rows();
        }
    }

    println!("Read {} rows in {} ms", rows, start.elapsed().as_millis());

    Ok(())
}