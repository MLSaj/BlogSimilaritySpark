package com.testing

import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import com.testing.WriteToMongoTest.{insert_arrays, read_mongo}
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, _}
import com.mongodb.spark.config.ReadConfig

object SimilarityUpdate {

  val sparkSession = SparkSession.builder()
    .appName("Similarity Update")
    .master("local")
    .getOrCreate()


  val mysql_host_name = "localhost"
  val mysql_port_no = "3306"
  val mysql_user_name = "root"
  val mysql_password = "root"
  val mysql_database_name = "similarity"
  val mysql_driver_class = "com.mysql.cj.jdbc.Driver"
  val mysql_table_name = "sim_table"
  val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

  val mysql_properties = new java.util.Properties
  mysql_properties.setProperty("driver", mysql_driver_class)
  mysql_properties.setProperty("user", mysql_user_name)
  mysql_properties.setProperty("password", mysql_password)





  def makeMongoURI(uri:String,database:String,collection:String) = (s"${uri}/${database}.${collection}")

  val mongoURI = "mongodb://000.000.000.000:27017"
  val Conf = makeMongoURI(mongoURI,"blog","articles")

  val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))


  // Uses the ReadConfig
  val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))

  def main(args: Array[String]): Unit = {

    val dataTest = read_mongo()
    insert_arrays(dataTest)

    //df3.show(false)
//    val df4 = df3.select("title", "markdown")
//
//    def last_element= udf((blog : String) => blog takeRight(1))
//
//    val df5 = df4.withColumn("id", last_element(col("title")))
//
//    //df5.show()
//
//    val tokenizer = new Tokenizer().setInputCol("markdown").setOutputCol("words")
//
//    val wordsData = tokenizer.transform(df5)
//
//    val hashingTF = new HashingTF()
//      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(28)
//    val featurizedData = hashingTF.transform(wordsData)
//
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("feature")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData)
//
//    val normalizer = new Normalizer().setInputCol("feature").setOutputCol("norm")
//
//    val data = normalizer.transform(rescaledData)
//
//    //data.show(20,false)
//    //data.write.mode(SaveMode.Overwrite).jdbc(ur)
//
//
//
//    val dot_udf =  udf((x: org.apache.spark.ml.linalg.Vector,y:org.apache.spark.ml.linalg.Vector) => x.dot(y))



//    val join = data.alias("i").join(data.alias("j"),
//      col("i.id") < col("j.id")).select(
//      col("i.title").alias("i"),
//      col("j.title").alias("j"),
//      dot_udf(col("i.norm"), col("j.norm")).alias("dot"))
//      .sort("i","j")
//
//
//    join.write.mode(SaveMode.Overwrite).jdbc(url=mysql_jdbc_url, table = mysql_table_name,mysql_properties);




  }


}
