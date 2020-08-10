import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, Tokenizer}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}



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

  val size = udf {
    (x: org.apache.spark.ml.linalg.SparseVector) => {
      //val vec = x.toSparse
      x.size.toInt
    }
  }
  val values = udf {
    (x: org.apache.spark.ml.linalg.SparseVector) => {
      //val vec = x.toSparse
      x.values
    }
  }

  val indices = udf {
    (x: org.apache.spark.ml.linalg.SparseVector) => {
      //val vec = x.toSparse
      x.indices
    }
  }

  def convert_spark(x:org.apache.spark.mllib.linalg.Vector) ={
    val sparse_representation = x.toSparse
  }

  def insert_arrays(data:DataFrame) = {
    val sparse_data = data.select("norm","title","id")

    val new_sizes = sparse_data.withColumn("size", size (col("norm")))

    val new_indices = new_sizes.withColumn("values", values(col("norm")))

    val new_values = new_indices.withColumn("indices", indices(col("norm")))
      .select("id","title","size","values","indices")

    new_values.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))

    //new_values.show(20,false)
   // new_values.printSchema()

  }


  def main(args: Array[String]): Unit = {
    //reads data from mongo and does some transformations
    val data = read_mongo()
    insert_arrays(data)
    //val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
    //new_data.show(20,false)
    //val norm_column = data.select("id", "norm")

    //val data = read_mongo()
    //insert_arrays(data)





    //sparse_data.show(20,false)
//    data.write.option("vectors", "blogs").mongo()
    //sparkSession.sparkContext.parallelize(data).saveToMongoDB()
    //val writeConfig = WriteConfig(Map("collection" -> "vectors", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkSession)))

    //MongoSpark.save(data, writeConfig)

    //new_values.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))

//    val someSchema = List(
//      StructField("number", IntegerType, true),
//      StructField("word", StringType, true),
//      StructField("array", ArrayType(IntegerType),true)
//    )
//
//    val someData = Seq(
//      Row(8, "bat", Array(1,2,3)),
//      Row(64, "mouse",Array(4,5,6)),
//      Row(-27, "horse",Array(1,1,1))
//    )
//
//    val someDF = sparkSession.createDataFrame(
//      sparkSession.sparkContext.parallelize(someData),
//      StructType(someSchema)
//    )
//
//    someDF.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.testDF")))






  }

}
