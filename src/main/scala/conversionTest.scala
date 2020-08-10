
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object conversionTest {


  def main(args: Array[String]): Unit = {
    val sparkSess = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true),
      StructField("array", ArrayType(IntegerType),true)
    )

    val someData = Seq(
      Row(8, "bat", Array(1,2,3)),
      Row(64, "mouse",Array(4,5,6)),
      Row(-27, "horse",Array(1,1,1))
    )

    val someDF = sparkSess.createDataFrame(
      sparkSess.sparkContext.parallelize(someData),
      StructType(someSchema)
    )

    someDF.show()

    someDF.printSchema()


    //someDF.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.vectors")))





  }
}
