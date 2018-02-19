
import java.io.{File, FileOutputStream}

import SortLocations.os
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object GenerateWKT {

  val os = new FileOutputStream(new File(s"progress_${System.currentTimeMillis()}.log"));


  def main(arg: Array[String]): Unit = {
//    runJob(100000)
    generateWkt(10)
    generateWkt(100)
    generateWkt(1000)
    generateWkt(10000)
    generateWkt(100000)
    generateWkt(1000000)
    generateWkt(10000000)
    generateWkt(100000000)
    generateWkt(1000000000)
  }

  def generateWkt(n: Long) {

    os.write(s"Processing $n locations ...\n".getBytes())
    os.flush()

    val now = System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[1]")
      .getOrCreate()

    val path = "/tmp/"

    spark.udf.register("ST_Point", (x: Double, y: Double) => Point(x,y))
    spark.udf.register("ST_AsText", (row: Row) => {
      val lat = row.getAs[Double](0)
      val lon = row.getAs[Double](1)
      s"POINT ($lat $lon)"
    })

    val df: DataFrame = spark.read.csv(path + s"locations_$n.csv")
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "lat")
      .withColumnRenamed("_c2", "lng")
      .repartition(1)

    df.createOrReplaceTempView("locations")

    // create WKT for each point
    val df2 = spark.sql("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations")

    // write output
    df2.write.mode(SaveMode.Overwrite).csv(s"/tmp/spark-generateWkt-$n.csv")

    val duration = (System.currentTimeMillis()-now)/1000.0
    val rowsPerSecond = n / duration

    os.write(s"Processed $n locations in $duration seconds ($rowsPerSecond rows per second\n)".getBytes())
    os.flush()

  }



}


case class Point(x: Double, y: Double)