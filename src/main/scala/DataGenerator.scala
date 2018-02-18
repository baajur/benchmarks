import java.io.{BufferedOutputStream, File, FileOutputStream}

import scala.collection.immutable

object DataGenerator {

  def main(arg: Array[String]): Unit = {

    def doGen(n: Long): Unit = {
      if (n>0) {
        generate(n)
        doGen(n*10)
      }
    }

    doGen(10)
  }

  def generate(n: Long) {

    println(s"Generating $n locations ...")
    val t1 = System.currentTimeMillis()
    val r = scala.util.Random
    val os = new BufferedOutputStream(new FileOutputStream(new File(s"/tmp/locations_$n.csv")))

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
