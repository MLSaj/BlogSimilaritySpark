//package streamTests
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Minutes, State, StateSpec}
//import user.{USession, UserEvent}
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
//import scala.collection.Map
//
//object StreamStateFour {
//
//  val sparkSession = SparkSession.builder()
//    .master("local")
//    .appName("StreamStateThree")
//    .getOrCreate()
//
//  import sparkSession.implicits._
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
//  def trackStateFunc(key: Int, event:Option[UserEvent], state: State[USession]): Option[(Int, USession)] = {
//    // Extract the previous state passed in (using getOrElse to handle exceptions)
//    val previousState = state.getOption.getOrElse(USession(Map[String, Boolean](), -7))
//
//    // Create a new state that increments the session length by one, adds this URL to the clickstream, and clamps the clickstream
//    // list to 10 items
//    state.update(previousState)
//
//
//    // Return a new key/value result.
//    Option((key.toInt, previousState))
//  }
//
//
//
//  def main(args: Array[String]): Unit = {
//    //new_data2.show(20,false)
//
//    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))
//
//
//    val userEventsStream = sparkSession.readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 12346)
//      .load()
//      .as[String]
//
//
//
//    val UserEventStream =
//      userEventsStream
//        .map(deserializeUserEvent)
//
//
//
//    userEventsStream
//
//
//  }
//
//}
