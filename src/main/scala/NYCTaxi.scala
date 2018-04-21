import org.apache.spark.sql.SparkSession

/**
  * Data: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
  */
object NYCTaxi {

  def main(arg: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[1]") // force single thread usage for fair comparison to DataFusion
      .getOrCreate()

    val df = spark.read
        .option("header", "true")
        .csv("/mnt/ssd/nyc_taxis/yellow_tripdata_2017-12.csv")

    df.show(10)

    df.createOrReplaceTempView("tripdata")


    test(spark, "SELECT COUNT(1) FROM tripdata")

    test(spark, "SELECT passenger_count, COUNT(1) " +
      "FROM tripdata " +
      "GROUP BY passenger_count")

    test(spark, "SELECT passenger_count, COUNT(1), MIN(CAST(fare_amount AS FLOAT)), MAX(CAST(fare_amount AS FLOAT)) " +
      "FROM tripdata " +
     // "WHERE (payment_type = '1' OR payment_type = '2') AND RateCodeID = '1' " +
      "GROUP BY passenger_count")
  }

  def test(spark: SparkSession, sql: String): Unit = {
    println(sql)
    val start = System.currentTimeMillis()
    val df2 = spark.sql(sql)
    df2.collect().foreach(println)
    val end = System.currentTimeMillis()
    println(s"Took ${end-start} ms")
    println("--")
  }

}
