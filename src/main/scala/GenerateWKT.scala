
import java.io.{File, FileOutputStream}

import SortLocations.os
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object GenerateWKT {

  val os = new FileOutputStream(new File(s"progress_${System.currentTimeMillis()}.log"));

  def main(arg: Array[String]): Unit = {
    if (arg.length == 0) {
      println("Missing args")
    }
    generateWkt(arg(0).toInt)
  }

  def generateWkt(n: Long) {

    os.write(s"Processing $n locations ...\n".getBytes())
    os.flush()

    val now = System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("GenerateWKT")
      .master("local[1]")
      .getOrCreate()

    val path = "/mnt/ssd/"

    spark.udf.register("ST_Point", (x: Double, y: Double) => Point(x,y))
    spark.udf.register("ST_AsText", (row: Row) => {
      val lat = row.getAs[Double](0)
      val lon = row.getAs[Double](1)
      s"POINT ($lat $lon)"
    })

    val df: DataFrame = spark.read.csv(path + s"locations_$n.csv")
      .coalesce(1)
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "lat")
      .withColumnRenamed("_c2", "lng")

    df.createOrReplaceTempView("locations")

    // create WKT for each point
    val df2 = spark.sql("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations")

    // write output
    df2.write.mode(SaveMode.Overwrite).csv(path + s"spark-generateWkt-$n.csv")

    val duration = (System.currentTimeMillis()-now)/1000.0
    val rowsPerSecond = n / duration

    os.write(s"Processed $n locations in $duration seconds ($rowsPerSecond rows per second\n)".getBytes())
    os.flush()

  }



}


case class Point(x: Double, y: Double)