import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Data: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
  */
object Benchmarks {

  def main(arg: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val parquetPath = "/home/andy/nyc-tripdata/parquet/yellow_tripdata_2009-01.parquet/part-00000-156e4a16-37be-44dc-b4e8-5155e08ce7d3-c000.snappy.parquet"

    loadParquet(spark, parquetPath)

    //    test(spark, "SELECT COUNT(1), MIN(fare_amount), MAX(fare_amount) FROM tripdata")

    val sql = "SELECT passenger_count, MIN(fare_amt), MAX(fare_amt) " +
      "FROM tripdata " +
      "GROUP BY passenger_count"

    /*
    [1,9493418,2.5,200.0]
[6,63817,2.5,106.9]
[3,624652,2.5,190.1]
[5,1263356,2.5,179.3]
[4,299288,2.5,187.7]
[113,1,13.3,13.3]
[2,2347163,2.5,200.0]
[0,718,2.5,45.0]
Took 2659 ms
     */



    benchmark(spark, sql)
  }

    //    test(spark, "SELECT COUNT(1) FROM tripdata")

    //    test(spark, "SELECT passenger_count, COUNT(1) " +
    //      "FROM tripdata " +
    //      "GROUP BY passenger_count")

//    test(spark, "SELECT COUNT(1), MIN(CAST(fare_amount AS FLOAT)), MAX(CAST(fare_amount AS FLOAT)) FROM tripdata")

//    benchmark(spark, "SELECT passenger_count, COUNT(1), MIN(CAST(fare_amount AS FLOAT)), MAX(CAST(fare_amount AS FLOAT)) " +
//      "FROM tripdata " +
//      "GROUP BY passenger_count")


    /*
[7,44,-70.0,77.75]
[3,425351,-169.0,550.0]
[8,36,0.8,100.0]
[0,58353,0.0,655.35]
[5,443643,-59.7,850.0]
[6,268944,-52.0,297.5]
[9,30,-9.0,99.99]
[1,6647126,-340.0,391911.78]
[4,217875,-60.0,590.0]
[2,1446874,-340.0,3029.0]
     */

    // 12 seconds CSV
    // 2 seconds Parquet


  def loadCsv(spark: SparkSession, path: String) {

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv(path)

    df.printSchema()

    df.createOrReplaceTempView("tripdata")
  }

  def loadParquet(spark: SparkSession, path: String) {
    val df = spark.read
      .parquet(path)

    df.printSchema()

    df.createOrReplaceTempView("tripdata")
  }

  def benchmark(spark: SparkSession, sql: String): Unit = {
    println(sql)
    val start = System.currentTimeMillis()
    val df2 = spark.sql(sql)
    df2.collect().foreach(println)
    val end = System.currentTimeMillis()
    println(s"Took ${end-start} ms")
    println("--")
  }

}
