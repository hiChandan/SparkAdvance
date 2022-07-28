import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
case class Languages(language: String, users: Int)
object BasicRevision {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkConnector.getSparkSession
    val columns = Seq("language", "users")
    val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    import spark.implicits._
//1
    val rdd = spark.sparkContext.parallelize(data)
    println("Using RDD to DF")
    val df = rdd.toDF
    df.show()
//2
    println("Using RDD to DF with column name")
    val df2 = rdd.toDF(columns: _*)
    val df2_alt = spark.createDataFrame(rdd).toDF(columns: _*)
    df2.show()
    df2_alt.show()
//3
    println("Using RDD to DF with Schema")
    val schema = StructType(
      Array(
        StructField("language", StringType, true),
        StructField("users", IntegerType, true)
      )
    )
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
    val df3 = spark.createDataFrame(rowRDD, schema)
    df3.show()

//4    Not so important
    import scala.collection.JavaConversions._
    val rowData =
      Seq(Row("Java", 20000), Row("Python", 100000), Row("Scala", 3000))
    val df4 = spark.createDataFrame(rowData, schema).toDF()
    df4.show()
//5
    // DataSet Compile Safety,uses Tungsten’s fast in-memory encoders
    println("Create Dataset using DS")
    val dsSimple = data.toDS()
    dsSimple.show()
    println("Create Dataset using Custom Type")
    val ds = df2.as[(String, Int)]
    val ds_alt = df2.as[Languages]
    ds.show()
//6
    println("Create Dataset using case class - Note Language in data 2")
    val data2 = Seq(
      Languages("Java", 20000),
      Languages("Python", 100000),
      Languages("Scala", 3000)
    )

    val ds2 = spark.sparkContext.parallelize(data2).toDS()
    ds2.show()
    val ds3 = spark.sparkContext.parallelize(data2).toDF().as[Languages]
    ds3.show()
//    ds3.filter(f => f.language == "Scala")  DataSet Advantage

//7
    println("Create DataFrame")
    val emptyDF = Seq.empty[Languages].toDF()
//    val emptyDF = Seq.empty[Languages].toDS()
    emptyDF.show()
  }
}
