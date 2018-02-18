
import java.io.{File, FileOutputStream}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SortLocations {

  val os = new FileOutputStream(new File(s"sortLocations_progress_${System.currentTimeMillis()}.log"))

  def main(arg: Array[String]): Unit = {
    sortLocations(10)
    sortLocations(100)
    sortLocations(1000)
    sortLocations(10000)
    sortLocations(100000)
    sortLocations(1000000)
    sortLocations(10000000)
    sortLocations(100000000)
    sortLocations(1000000000)
  }

  def sortLocations(n: Long) {

    os.write(s"Processing $n locations ...\n".getBytes())
    os.flush()

    val now = System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[1]") // force single thread usage for fair comparison to DataFusion
      .getOrCreate()

    val path = "/tmp/"

    val df: DataFrame = spark.read.csv(path + s"locations_$n.csv")
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "lat")
      .withColumnRenamed("_c2", "lng")
      .repartition(1)

    df.createOrReplaceTempView("locations")

    // create WKT for each point
    val df2 = spark.sql("SELECT * FROM locations ORDER BY lat, lng")

    // write output
    df2.write.mode(SaveMode.Overwrite).csv(s"/tmp/spark-locations-sorted-$n.csv")

    val duration = (System.currentTimeMillis()-now)/1000.0
    val rowsPerSecond = n / duration

    os.write(s"Processed $n locations in $duration seconds ($rowsPerSecond rows per second\n)".getBytes())
    os.flush()

  }



}
