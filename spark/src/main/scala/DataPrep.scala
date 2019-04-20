import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Utility for converting CSV to Parquet and repartitioning files.
  */
object DataPrep {

  def main(args: Array[String]): Unit = {
    val csvPath = "~/nyc-tripdata/original"
    val parquetPath = "~/nyc-tripdata/parquet"

    downloadFiles()
  }


  def downloadFiles(): Unit = {
    for (year <- 2009 to 2008) {
      for (month <- 1 to 12) {
        val url = s"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$year-$month.csv"
        println(url)
      }
    }
  }

  /** Load CSV with schema inferred so that parquet file has correct types */
  def convertToParquet(csvPath: String, parquetPath: String) {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.printSchema()

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)
  }

}
