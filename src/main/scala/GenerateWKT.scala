
import java.io.{File, FileOutputStream}

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJob1 {

  val os = new FileOutputStream(new File(s"progress_${System.currentTimeMillis()}.log"));


  def main(arg: Array[String]): Unit = {
//    runJob(100000)
    runJob(10)
    runJob(100)
    runJob(1000)
    runJob(10000)
    runJob(100000)
    runJob(1000000)
    runJob(10000000)
    runJob(100000000)
    runJob(1000000000)
  }

  def runJob(n: Long) {

    os.write(s"Processing $n locations ...\n".getBytes())
    os.flush()

    val now = System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("Simple Application")
        .enableHiveSupport()
      .master("local[1]")
      .getOrCreate()

    val path = "/home/andy/Documents/bigdata/"

    spark.udf.register("ST_Point", (x: Double, y: Double) => Point(x,y))
    spark.udf.register("ST_AsText", (p:Point) => s"POINT (${p.x}, ${p.y})")

    val df: DataFrame = spark.read.csv(path + s"locations_$n.csv")
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "lat")
      .withColumnRenamed("_c2", "lng")
      .repartition(1)

    df.createOrReplaceTempView("locations")

    // create WKT for each point
    val df2 = spark.sql("SELECT ST_AsText(ST_Point(lat, lng)) FROM locations")
    df2.printSchema()

    // write output
    df2.write.csv("temp" + System.currentTimeMillis() + ".csv")

    os.write(s"Processed $n locations in ${(System.currentTimeMillis()-now)/1000.0} seconds\n".getBytes())
    os.flush()

  }



}


case class Point(x: Double, y: Double)