import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import scalaz.{-\/, \/, \/-}
import user.{UserEvent, UserSession}

import scala.collection.{mutable, _}
import argonaut.Argonaut._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.collection.Map


object StreamState{

  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
  implicit val userSessionEncoder: Encoder[Option[UserSession]] =
    Encoders.kryo[Option[UserSession]]

  def deserializeUserEvent(json: String): UserEvent = {
    json.decodeEither[UserEvent] match {
      case Right(userEvent)=> userEvent
      case Left(error) =>
        println(s"Failed to parse user event: $error")
        UserEvent.empty
    }
  }



  val mongoURI = "mongodb://000.000.000.000:27017"

  val Conf = makeMongoURI(mongoURI,"blog","articles")
  val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))


  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/blog.articles")
    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/blog.vectors")
    .getOrCreate()


  val makeSparseMapUdf = udf {
    (size:Int, values:mutable.WrappedArray[Double], indices:mutable.WrappedArray[Int]) => {
      //val vec = x.toSparse
      //Vectors.sparse(size,indices,values)
      //SparseVector(size,indices,values)
      // size
      //Vectors.dense(indices.toArray)
      val new_indices = indices.toArray
      //val new_values = Vectors.dense(values.toArray)
      val new_values = values.toArray
      val filter_values = new_values.filter(element => (element > 0.0 ))

      Vectors.sparse(size,new_indices,values.toArray)


    }
  }


  // Uses the ReadConfig
  //val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))
  val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
  val with_sparse = new_data.withColumn("sparse", makeSparseMapUdf(new_data("size"),
    new_data("values"),new_data("indices")))



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

  def add_two_sparse_option(option_x:Option[org.apache.spark.ml.linalg.Vector], y:org.apache.spark.ml.linalg.Vector)
  : org.apache.spark.ml.linalg.Vector= {

    val x = option_x.getOrElse(Vectors.sparse(28,Array(0),Array(0.0001)))
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

    def compare(size:Int, indices:Array[Int], values:Array[Double]) = udf {
      (y:org.apache.spark.ml.linalg.Vector) => {
        val x = Vectors.sparse(size,indices,values)
        x.dot(y)

      }
    }


  def compare_sparse(x:org.apache.spark.ml.linalg.Vector) = udf {
    (y:org.apache.spark.ml.linalg.Vector) => {
      x.dot(y)
    }
  }

  def updateSessionEvents(
                           id: Int,
                           userEvents: Iterator[UserEvent],
                           state: GroupState[UserSession]): Option[UserSession] = {
    if (state.hasTimedOut) {
      // We've timed out, lets extract the state and send it down the stream

      state.remove()
      state.getOption
    } else {
      /*
       New data has come in for the given user id. We'll look up the current state
       to see if we already have something stored. If not, we'll just take the current user events
       and update the state, otherwise will concatenate the user events we already have with the
       new incoming events.
       */
      import sparkSession.implicits._

      val empty_vector = Vectors.sparse(28,Array(0),Array(0.00000001))
      //val empty_map:Predef.Map[String, Boolean] = Map()
      val empty_map = Map[String, Boolean]()

      val empty_rec: Array[String] = Array()
      val empty_session = UserSession(empty_map, Option(empty_vector),empty_rec)

      val optionState = state.getOption


     val currentState =  optionState.getOrElse(empty_session)


      val vector_events = userEvents.map(
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

      val reduced = vector_events.reduce((eventN, eventX) => {
          val new_map = eventN._1 ++ eventX._1
          val new_vector = add_two_sparse(eventN._2, eventX._2)
          (new_map, new_vector)
      })


      val total_visits = currentState.visited ++ reduced._1
      val total_vector = add_two_sparse_option(currentState.current_vector, reduced._2)
      val visits_array: Array[String] = total_visits.map(k => k._1.toString).toArray

      //compute recommendations
      val filter_seen = with_sparse.filter(!with_sparse("title").isin(visits_array))

      val sim = filter_seen.withColumn("similarities", compare_sparse(total_vector)
      (filter_seen("sparse")))

      val order = sim.select("title").orderBy(desc("similarities")).limit(3)
      .map(r => r(0).asInstanceOf[String]).collect()


      val new_user_session = UserSession(total_visits, Option(total_vector), order)
      state.update(new_user_session)
      state.setTimeoutDuration("5 minute")
      state.getOption


        }
      }









  def main(args: Array[String]): Unit = {

    sparkSession.sparkContext.setLogLevel("WARN")
//    val UserEventEncoder = Encoders.product[UserEvent]
//
     import sparkSession.implicits._
//
//
    val userEventsStream = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[UserEvent]






    val finishedUserSessionsStream: Dataset[UserSession] =
      userEventsStream
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(
          updateSessionEvents)
        .flatMap(userSession => userSession)

     finishedUserSessionsStream.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("checkpointLocation", "checkpoint")
      .option("truncate",false)
      .start()
      .awaitTermination()







//        with_sparse.printSchema()
//
//        //with_sparse.show(20,false)
//
//        import sparkSession.implicits._
//
//
//        val small_df = with_sparse.filter($"id" === 1)
//        val values:Array[Double] = small_df.select("values").as[Array[Double]].collect()(0)
//        val indices:Array[Int] =  small_df.select("indices").as[Array[Int]].collect()(0)
//        val size:Int = small_df.select("size").as[Int].collect()(0)
//
//        compare _
//
//        with_sparse.withColumn("similarities",compare(size,indices,values)(with_sparse("sparse"))).show(20,false)


  }


}

