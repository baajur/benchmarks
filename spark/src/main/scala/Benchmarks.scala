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

    val parquetPath = "/home/andy/nyc-tripdata/parquet"

    loadParquet(spark, parquetPath)

    val sql = "SELECT passenger_count, MIN(fare_amt), MAX(fare_amt) " +
      "FROM tripdata " +
      "GROUP BY passenger_count"

    benchmark(spark, sql)
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