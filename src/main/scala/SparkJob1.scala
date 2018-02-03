
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJob1 {

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[1]")
      .getOrCreate()

    val df: DataFrame = spark.read.csv("/home/andy/Documents/bigdata/locations_10.csv")

    df.createOrReplaceGlobalTempView("locations")

    val df2 = spark.sql("SELECT ST_Point(lat, lng) FROM locations")
    df2.write.csv("temp" + System.currentTimeMillis() + ".csv")

  }



}
