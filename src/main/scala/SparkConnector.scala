import org.apache.spark.sql.SparkSession

object SparkConnector {
  private val SPARK_APP_NAME = "SparkAdvance"
  private val SPARK_MASTER = "local[*]"
  def getSparkSession: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(SPARK_APP_NAME)
      .master(SPARK_MASTER)
      .getOrCreate()

    spark
  }

}
