//package streamTests
//
//import com.mongodb.spark.config.ReadConfig
//import com.testing.SimilarityUpdate.makeMongoURI
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.udf
//
//import scala.collection.{Map, mutable}
//
//object DataSetPlayground {
//  val mongoURI = "mongodb://000.000.000.000:27017"
//
//  val Conf = makeMongoURI(mongoURI,"blog","articles")
//  val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))
//
//
//  val sparkSession = SparkSession.builder()
//    .master("local")
//    .appName("MongoSparkConnectorIntro")
//    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/blog.articles")
//    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/blog.vectors")
//    .getOrCreate()
//
//
//  val makeSparseMapUdf = udf {
//    (size:Int, values:mutable.WrappedArray[Double], indices:mutable.WrappedArray[Int]) => {
//      //val vec = x.toSparse
//      //Vectors.sparse(size,indices,values)
//      //SparseVector(size,indices,values)
//      // size
//      //Vectors.dense(indices.toArray)
//      val new_indices = indices.toArray
//      //val new_values = Vectors.dense(values.toArray)
//      val new_values = values.toArray
//      val filter_values = new_values.filter(element => (element > 0.0 ))
//
//      Vectors.sparse(size,new_indices,values.toArray)
//
//
//    }
//  }
//
//  val makeSparseMap =
//    (size:Int, values:mutable.WrappedArray[Double], indices:mutable.WrappedArray[Int]) => {
//      //val vec = x.toSparse
//      //Vectors.sparse(size,indices,values)
//      //SparseVector(size,indices,values)
//      // size
//      //Vectors.dense(indices.toArray)
//      val new_indices = indices.toArray
//      //val new_values = Vectors.dense(values.toArray)
//      val new_values = values.toArray
//      val filter_values = new_values.filter(element => (element > 0.0 ))
//
//      Vectors.sparse(size,new_indices,values.toArray)
//
//
//    }
//
//
//  // Uses the ReadConfig
//  //val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))
//  val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
//  val with_sparse = new_data.withColumn("sparse", makeSparseMapUdf(new_data("size"),
//    new_data("values"),new_data("indices")))
//
//
//  def main(args: Array[String]): Unit = {
//    //with_sparse.show(20,false)
//    import sparkSession.implicits._
//    val current_event = with_sparse.filter($"title" === "Blog1").limit(1)
//
//
//    case class IndexCase(i:Array[Int])
//
//    case class vals(v:Array[Double])
//
//    case class sizes(s:Int)
//
//
//
//    val indices = current_event.select("indices").limit(1)
//
//    val values = current_event.select("values").limit(1)
//
//    val size = current_event.select("size").limit(1)
//
//
//
//
//
//
//
//
//
//
//
//
//
//  }
//
//
//
//}
