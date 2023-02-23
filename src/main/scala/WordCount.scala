import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._


object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.read.textFile("./README.md").rdd

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val a = counts.collect()
    var total = 0

    a.foreach {
      case (values, key) => {
        breakable {
          total += 1
          if (values == "") {
            total -= 1
            break
          }
          println(values)
        }
      }
    }
    println(s"total word count: $total")

    spark.stop()
  }

}
