//package streamTests
//
//import com.mongodb.spark.config.ReadConfig
//import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
//import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
//import user.{USession, UserEvent, UserSession}
//
//import com.mongodb.spark.config.ReadConfig
//import com.testing.SimilarityUpdate.makeMongoURI
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.sql.functions.{desc, udf}
//import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
//import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
//import user.{UserEvent, UserSession}
//import argonaut.Argonaut._
//import scala.collection.{Iterator, Map, mutable}
//import com.mongodb.spark.config.ReadConfig
//import com.mongodb.spark.sql._
//import com.testing.SimilarityUpdate.makeMongoURI
//
//import scala.collection.{Iterator, Map}
//
//object StreamStateThree {
//
//  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
//  implicit val userSessionEncoder: Encoder[Option[UserSession]] =
//    Encoders.kryo[Option[UserSession]]
//
//  val sparkSession = SparkSession.builder()
//    .master("local")
//    .appName("StreamStateThree")
//    .getOrCreate()
//
//  sparkSession.sparkContext.setLogLevel("WARN")
//
//  import sparkSession.implicits._
//
//  val sc = sparkSession.sparkContext
//
//  val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
//
//  sparkSession.sparkContext.broadcast(new_data)
//
//  def updateUserStateWithEvent(statez:USession, event:UserEvent):USession = {
//    println("Updating")
//
//    val empty_map = Map[String, Boolean]()
//    val empty_rec: Array[String] = Array("")
//    val current_event = new_data.filter($"title" === event.url)
//    val size:Int = current_event.select("size").as[Int].collect()(0)
//    //.limit(1)
//    val empty_session = USession(empty_map,-7)
//    empty_session
//  }
//
//
//  def updateAcrossEvents(user:Int,
//                         events: Iterator[UserEvent],
//                         oldState: GroupState[USession]):USession = {
//
//    var state:USession = if (oldState.exists) {
//      println("State exists with the following visited")
//      oldState.get
//    }
//    else {
//      println("State does not exist")
//
//
//      val empty_map = Map[String, Boolean]()
//      val empty_session = USession(empty_map,-7)
//      empty_session
//    }
//    // we simply specify an old date that we can compare against and
//    // immediately update based on the values in our data
//
//    for (event <- events) {
//      state = updateUserStateWithEvent(state, event)
//      oldState.update(state)
//    }
//    state
//  }
//
//
//  def deserializeUserEvent(json: String): UserEvent = {
//    json.decodeEither[UserEvent] match {
//      case Right(userEvent) => userEvent
//      case Left(error) =>
//        println(s"Failed to parse user event: $error")
//        UserEvent.empty
//    }
//  }
//
//
//def main(args: Array[String]): Unit = {
//    //new_data2.show(20,false)
//  val userEventsStream = sparkSession.readStream
//    .format("socket")
//    .option("host", "localhost")
//    .option("port", 12346)
//    .load()
//    .as[String]
//
//
//
//  val finishedUserSessionsStream =
//    userEventsStream
//      .map(deserializeUserEvent)
//      .groupByKey(_.id)
//      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(
//        updateAcrossEvents)
//
//
//  finishedUserSessionsStream.writeStream
//    .outputMode(OutputMode.Update())
//    .format("console")
//    .option("checkpointLocation", "checkpoint")
//    .option("truncate",false)
//    .start()
//    .awaitTermination()
//
//
//}
//
//}
