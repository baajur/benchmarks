package io.andygrove.spark

import org.apache.spark.sql.SparkSession

object Benchmarks {

  def run(path: String, sql: String, iterations: Int): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    loadParquet(spark, path)

    val durations = for (i <- 1 to iterations) yield {
      println(s"**** Running iteration $i")
      val t1 = System.currentTimeMillis()
      spark.sql(sql).collect().foreach(println)
      val t2 = System.currentTimeMillis()
      (t2-t1)
    }

    spark.close()

    durations.zipWithIndex.foreach {
      case (duration,iter) =>
        println(s"Iteration ${iter+1} took ${duration/1000.0} seconds")
    }

  }

  def loadParquet(spark: SparkSession, path: String) {
    val df = spark.read
      .parquet(path)

    df.printSchema()

    df.createOrReplaceTempView("tripdata")
  }


}
