import org.apache.spark.sql.SparkSession

object RDDInitialAdd {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val distData = spark.sparkContext.parallelize(data, 5)

    println(distData.reduce((a, b) => a + b))
    spark.stop()
  }

}
