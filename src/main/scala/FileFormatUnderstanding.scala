import com.typesafe.scalalogging.LazyLogging
object FileFormatUnderstanding extends LazyLogging {
  case class Numbers(base: Int, value: Int)
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors

    val spark = SparkConnector.getSparkSession
    logger.info("\nLoading movie names...")
    import spark.implicits._
    val s = (1 to 10000).toList
    val ds = s.map(x => Numbers(x % 10, x)).toDF().as[Numbers]
    val dsParition = ds
    logger.debug("Stared Logging ")
    dsParition.write.csv("data/output/csv")
    logger.debug("CSV Logging ")
    dsParition.write.parquet("data/output/parquet")
    logger.debug("Parquet Logging ")
    dsParition.write.orc("data/output/orc")
    logger.debug("ORC Logging ")
    logger.debug("Ended Logging ")
    //    dsParition.write.partitionBy("base").csv("data/output/csv")
//    dsParition.write.partitionBy("base").parquet("data/output/parquet")
//    dsParition.write.partitionBy("base").orc("data/output/orc")

    println("Completed...")
  }
}
