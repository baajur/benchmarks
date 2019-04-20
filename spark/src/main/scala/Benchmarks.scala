import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Data: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
  */
object Benchmarks {

  val spark: SparkSession = SparkSession.builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  def main(arg: Array[String]): Unit = {
    val csvPath = "/home/andy/nyc-tripdata/original"
    val parquetPath = "/home/andy/nyc-tripdata/parquet"

    loadCsv(csvPath)
    //loadParquet(parquetPath)

    //    test(spark, "SELECT COUNT(1), MIN(fare_amount), MAX(fare_amount) FROM tripdata")

    val sql = "SELECT passenger_count, COUNT(1), MIN(fare_amount), MAX(fare_amount) " +
      "FROM tripdata " +
      "GROUP BY passenger_count"

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


  def loadCsv(path: String) {

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv(path)

    df.printSchema()

    df.createOrReplaceTempView("tripdata")
  }

  def loadParquet(path: String) {
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
