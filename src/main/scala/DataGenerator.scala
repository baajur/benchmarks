import java.io.{BufferedOutputStream, File, FileOutputStream}

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable
import scala.util.Random

object DataGenerator {

  def main(arg: Array[String]): Unit = {

    generateAllTypes()

//    def doGen(n: Long): Unit = {
//      if (n>0) {
//        generate(n)
//        doGen(n*10)
//      }
//    }
//
//    doGen(10)

    //generate(10000000)
  }
  
  def generateAllTypes(): Unit = {

    val spark = SparkSession.builder
      .appName("GenerateWKT")
      .master("local[1]")
      .getOrCreate()

    val r = new Random(System.currentTimeMillis())

//    val schema = StructType(Seq(
//      StructField("c_bool", DataTypes.BooleanType),
//      StructField("c_uint8", DataTypes.ByteType),
//      StructField("c_uint16", DataTypes.ShortType),
//      StructField("c_uint32", DataTypes.IntegerType),
//      StructField("c_uint64", DataTypes.LongType),
//      StructField("c_int8", DataTypes.ByteType),
//      StructField("c_int16", DataTypes.ShortType),
//      StructField("c_int32", DataTypes.IntegerType),
//      StructField("c_int64", DataTypes.LongType),
//      StructField("c_float32", DataTypes.FloatType),
//      StructField("c_float64", DataTypes.DoubleType)
//    ))

    val values = for (_ <- 0  until 256) yield {
      generateRandomRow(r)
    }

    val df = spark.createDataFrame(values)
    df.show(10)

    df.write.mode(SaveMode.Overwrite).csv("/mnt/ssd/all_types_flat.csv")
    df.write.mode(SaveMode.Overwrite).parquet("/mnt/ssd/all_types_flat.parquet")

  }

  def generateRandomRow(r: Random): AllTypes = {
    AllTypes(r.nextBoolean(),
      r.nextInt(2^7).toByte.abs, r.nextInt(2^15).toShort.abs, r.nextInt(2^31).abs, r.nextInt().abs,
      r.nextInt().toByte, r.nextInt().toShort, r.nextInt(), r.nextLong(),
      r.nextFloat(), r.nextDouble(), r.nextString(20)
    )
  }


  def generate(n: Long) {

    println(s"Generating $n locations ...")
    val t1 = System.currentTimeMillis()
    val r = scala.util.Random
    val os = new BufferedOutputStream(new FileOutputStream(new File(s"/mnt/ssd/input/locations_$n.csv")), 8*1024*1024)

    var i : Long = 0
    while (i < n) {
      val (lat, lng) = (r.nextDouble() * 180 - 90, r.nextDouble() * 360 - 180)
      os.write(s"$i,$lat,$lng\n".getBytes)
      i += 1
    }

    os.close()
    val t2 = System.currentTimeMillis()
    println(s"Generated $n locations in ${(t2-t1)/1000.0} seconds.")
    println()

  }

}

case class AllTypes(c_bool: Boolean,
                    c_uint8: Byte, c_uint16: Short, c_uint32: Int, c_uint64: Long,
                    c_int8: Byte, c_int16: Short, c_int32: Int, c_int64: Long, 
                    c_float32: Float, c_float64: Double, c_utf8: String)

case class NestedAllTypes(c_1: AllTypes, c_2: AllTypes)

