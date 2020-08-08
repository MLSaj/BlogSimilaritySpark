import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object WriteToMongoTest {

  val mongoURI = "mongodb://000.000.000.000:27017"
  val Conf = makeMongoURI(mongoURI,"blog","articles")
  val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))


  val sparkSess = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.output.uri", "mongodb://000.000.000.000:27017/blog.vectors")
    .getOrCreate()



  // Uses the ReadConfig
  val df3 = sparkSess.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))
  def read_mongo() = {
    val df4 = df3.select("title", "markdown")

    def last_element= udf((blog : String) => blog takeRight(1))

    val df5 = df4.withColumn("id", last_element(col("title")))

    //df5.show()

    val tokenizer = new Tokenizer().setInputCol("markdown").setOutputCol("words")

    val wordsData = tokenizer.transform(df5)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(28)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("feature")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val normalizer = new Normalizer().setInputCol("feature").setOutputCol("norm")

    val data = normalizer.transform(rescaledData)
    data
  }

  def main(args: Array[String]): Unit = {
    val data = read_mongo()
    data.show(20,false)
//    data.write.mode("append").mongo()

  }

}
