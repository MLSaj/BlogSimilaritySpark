import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object WriteToMongoTest {

  val mongoURI = "mongodb://000.000.000.000:27017"

  val Conf = makeMongoURI(mongoURI,"blog","articles")
  val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))


  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/blog.articles")
    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/blog.vectors")
    .getOrCreate()



  // Uses the ReadConfig
  val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))
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

  val makeSparseMapUdf = udf {
    (x:org.apache.spark.ml.linalg.SparseVector) => {
      //val vec = x.toSparse
      x.indices
        .map((index) => (index.toString, x.toArray(index)))
        .toMap
    }
  }

  def convert_spark(x:org.apache.spark.mllib.linalg.Vector) ={
    val sparse_representation = x.toSparse


  }

  def main(args: Array[String]): Unit = {
    //reads data from mongo and does some transformations
    val data = read_mongo()
    data.show(20,false)
    val norm_column = data.select("id", "norm")
    val sparse_data = data.withColumn("sparse_rep", makeSparseMapUdf (col("norm")))
      .select("id","title","sparse_rep")

    //sparse_data.show(20,false)
//    data.write.option("vectors", "blogs").mongo()
    //sparkSession.sparkContext.parallelize(data).saveToMongoDB()
    //val writeConfig = WriteConfig(Map("collection" -> "vectors", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkSession)))

    //MongoSpark.save(data, writeConfig)
    sparse_data.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.vectors")))




  }

}
