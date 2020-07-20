// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Instant;

extern crate ballista;

use ballista::arrow::datatypes::{Schema, Field, DataType};
use ballista::arrow::record_batch::RecordBatch;
use ballista::arrow::util::pretty;
use ballista::dataframe::{max, Context};
use ballista::datafusion::datasource::csv::CsvReadOptions;
use ballista::datafusion::logicalplan::*;
use ballista::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    //TODO use command-line args
    let path = "/mnt/tpch/100/lineitem";
    let executor_host = "localhost";
    let executor_port = 50051;
    let query_no = 1;

    let start = Instant::now();
    let ctx = Context::remote(executor_host, executor_port, HashMap::new());

    let results = match query_no {
        1 => q1(&ctx, path).await?,
        _ => unimplemented!(),
    };

    // print the results
    pretty::print_batches(&results)?;

    println!("Distributed query took {} ms", start.elapsed().as_millis());

    Ok(())
}

/// TPCH Query 1.
///
/// The full SQL is:
///
/// select
/// 	l_returnflag,
/// 	l_linestatus,
/// 	sum(l_quantity) as sum_qty,
/// 	sum(l_extendedprice) as sum_base_price,
/// 	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
/// 	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
/// 	avg(l_quantity) as avg_qty,
/// 	avg(l_extendedprice) as avg_price,
/// 	avg(l_discount) as avg_disc,
/// 	count(*) as count_order
/// from
/// 	lineitem
/// where
/// 	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
/// group by
/// 	l_returnflag,
/// 	l_linestatus
/// order by
/// 	l_returnflag,
/// 	l_linestatus;
///
async fn q1(ctx: &Context, path: &str) -> Result<Vec<RecordBatch>> {
    let schema = lineitem_schema();
    let options = CsvReadOptions::new().delimiter(b'|').schema(&schema);

    // TODO this is WIP and not the real query yet

    ctx.read_csv(path, options, None)?
       // .filter(col("l_shipdate").lt(&lit_str("1998-12-01")))? // should be l_shipdate <= date '1998-12-01' - interval ':1' day (3)
        .aggregate(
            vec![col("l_returnflag"), col("l_linestatus")],
            vec![
                max(col("l_quantity")),      // should be sum(l_quantity) as sum_qty
                max(col("l_extendedprice")), // should be sum(l_extendedprice) as sum_base_price
                max(col("l_extendedprice")), // should be sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
                max(col("l_quantity")), // should be sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
                max(col("l_quantity")), // should be avg(l_quantity) as avg_qty
                max(col("l_extendedprice")), // should be avg(l_extendedprice) as avg_price
                max(col("l_discount")), // should be avg(l_discount) as avg_disc
                max(col("l_quantity")), // should be count(*) as count_order
            ],
        )?
        //.sort()?
        .collect()
        .await
}

fn lineitem_schema() -> Schema {
    Schema::new(vec![
        Field::new("l_orderkey", DataType::UInt32, true),
        Field::new("l_partkey", DataType::UInt32, true),
        Field::new("l_suppkey", DataType::UInt32, true),
        Field::new("l_linenumber", DataType::UInt32, true),
        Field::new("l_quantity", DataType::Float64, true),
        Field::new("l_extendedprice", DataType::Float64, true),
        Field::new("l_discount", DataType::Float64, true),
        Field::new("l_tax", DataType::Float64, true),
        Field::new("l_returnflag", DataType::Utf8, true),
        Field::new("l_linestatus", DataType::Utf8, true),
        Field::new("l_shipdate", DataType::Utf8, true),
        Field::new("l_commitdate", DataType::Utf8, true),
        Field::new("l_receiptdate", DataType::Utf8, true),
        Field::new("l_shipinstruct", DataType::Utf8, true),
        Field::new("l_shipmode", DataType::Utf8, true),
        Field::new("l_comment", DataType::Utf8, true),
    ])
}