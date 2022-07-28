import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, col, udf}
object WindowingExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val data = Seq(
      (1, 2022, 100),
      (2, 2022, 150),
      (3, 2022, 150),
      (4, 2022, 250),
      (5, 2022, 890),
      (6, 2022, 900),
      (7, 2022, 110),
      (8, 2022, 100),
      (9, 2022, 200),
      (10, 2022, 310),
      (11, 2022, 104),
      (12, 2022, 210),
      (1, 2021, 100),
      (2, 2021, 150),
      (3, 2021, 150),
      (4, 2021, 250),
      (5, 2021, 890),
      (6, 2021, 900),
      (7, 2021, 110),
      (8, 2021, 100),
      (9, 2021, 200),
      (10, 2021, 310),
      (11, 2021, 104),
      (12, 2021, 210)
    )
    val rdd = data.map(x => Sales(x._1, x._2, x._3))
    val spark = SparkConnector.getSparkSession
    import spark.implicits._
    val quarter = (m: Int) => {
      m match {
        case a if a <= 3 => 1
        case b if b <= 6 => 2
        case b if b <= 9 => 3
        case _           => 4
      }
    }

    val quarterUDF = udf(quarter)
    val salesDS = spark.sparkContext.parallelize(rdd).toDS.as[Sales]
    val salesDSWithQuarter =
      salesDS.withColumn("quarter", quarterUDF(col("month")))
    val w = Window
      .partitionBy("year", "quarter")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    salesDSWithQuarter
      .withColumn("Cumulative Sum", sum("amount") over w)
      .sort("year", "month")
      .show()

  }
}

case class Sales(month: Int, year: Int, amount: Int)
