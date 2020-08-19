package streamTests

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, udf}
import user.UserEvent

import com.mongodb.spark.config.ReadConfig
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import user.{UserEvent, UserSession}
import argonaut.Argonaut._
import scala.collection.{Iterator, Map, mutable}
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import com.testing.SimilarityUpdate.makeMongoURI

import scala.collection.{Map, mutable}

object conversionTest {


  val makeSparseMapUdf = udf {
    (size: Int, values: mutable.WrappedArray[Double], indices: mutable.WrappedArray[Int]) => {
      //val vec = x.toSparse
      //Vectors.sparse(size,indices,values)
      //SparseVector(size,indices,values)
      // size
      //Vectors.dense(indices.toArray)
      val new_indices = indices.toArray
      //val new_values = Vectors.dense(values.toArray)
      val new_values = values.toArray
      val filter_values = new_values.filter(element => (element > 0.0))

      Vectors.sparse(size, new_indices, values.toArray)


    }
  }

  def add_two_sparse(x:org.apache.spark.ml.linalg.Vector, y:org.apache.spark.ml.linalg.Vector)
  : org.apache.spark.ml.linalg.Vector= {


    //val y = option_y.getOrElse(Vectors.sparse(28,Array(0),Array(0.0001)))
    val array_x = x.toArray
    val array_y = y.toArray
    val size = array_x.size
    val add_array = (array_x, array_y).zipped.map(_ + _)
    var i = -1;
    val new_indices_pre = add_array.map( (element:Double) => {
      i = i + 1
      if(element > 0.0)
        i
      else{
        -1
      }
    })

    val new_indices:Array[Int] = new_indices_pre.filter(element => element != -1)

    val final_add = add_array.filter(element => element > 0.0)
    Vectors.sparse(size,new_indices,final_add)
  }

  def compare_sparse(x:org.apache.spark.ml.linalg.Vector) = udf {
    (y:org.apache.spark.ml.linalg.Vector) => {
      x.dot(y)
    }
  }




def main(args: Array[String]): Unit = {
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("TestingStuff")
    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/blog.articles")
    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/blog.vectors")
    .getOrCreate()




  // Uses the ReadConfig
  //val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))
  val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
  val with_sparse = new_data.withColumn("sparse", makeSparseMapUdf(new_data("size"),
    new_data("values"), new_data("indices")))


  with_sparse.show(20,false)

  val event1: UserEvent = UserEvent(1, "Blog4")
  val event2: UserEvent = UserEvent(1, "Blog6")
  val event3: UserEvent = UserEvent(1, "Blog3")

  val eventIterator: Iterator[UserEvent] = Iterator(event1, event2, event3)

  //println(add_id.id)

  import sparkSession.implicits._


  case class Tfvector(x:org.apache.spark.ml.linalg.Vector)

  val eventVectors = eventIterator.map(
    UserEvent => {


      val current_event = with_sparse.filter($"title" === UserEvent.url)

      val values: Array[Double] = current_event.select("values").as[Array[Double]].collect()(0)
      val indices: Array[Int] = current_event.select("indices").as[Array[Int]].collect()(0)
      val size: Int = current_event.select("size").as[Int].collect()(0)
      val current_vector = Vectors.sparse(size, indices, values)


      val user_map: Map[String, Boolean] = Map(UserEvent.url -> true)

      (user_map, current_vector)

    }
  )

  //println(eventVectors.length)





  val reduced = eventVectors.reduce((eventN, eventX) => {
    val new_map = eventN._1 ++ eventX._1
    val new_vector = add_two_sparse(eventN._2, eventX._2)
    (new_map, new_vector)
  })



  val sparse_vector = reduced._2
  val map = reduced._1
  println(sparse_vector)
  println("Okay!")
  println(map)

  val visits_array: List[String] = map.map(k => k._1).toList

  val filter_seen = with_sparse.filter(!with_sparse("title").isin(visits_array:_*))


  //filter_seen.show(20,false)

  val sim = filter_seen.withColumn("similarities", compare_sparse(sparse_vector)
      (with_sparse("sparse")))


  val order:Array[String] = sim.select("title").orderBy(desc("similarities")).limit(2)
    .map(r => r(0).asInstanceOf[String]).collect()


  order.map(o => println(o))



  val collection = List(1, 3, 2, 5, 4, 7, 6)


  val collection2 = Iterator(1, 2, 3, 4, 5, 6)

}
}
