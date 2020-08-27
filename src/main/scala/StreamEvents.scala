import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import com.mongodb.spark.config.ReadConfig
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import user.{UserEvent, UserSession}
import org.apache.spark.sql.cassandra._
import argonaut.Argonaut._

import scala.collection.{Iterator, Map, mutable}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.joda.time.DateTime



object StreamEvents {

  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
  implicit val userSessionEncoder: Encoder[Option[UserSession]] =
    Encoders.kryo[Option[UserSession]]

  val cassandra_connection_host = "localhost"
  val cassandra_connection_port = "9042"

  val cassandra_keyspace_name = "blog"
  val cassandra_table_name = "session"

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("StreamEvents")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  def updateUserStateWithEvent(user:Int, statez:UserSession, eventz:UserEvent):UserSession = {
    val option_state = Option(statez)

    val state = option_state.getOrElse(UserSession(user,List.empty[String]))

    val option_event = Option(eventz)

    var path_visited = state.visited

    val event = option_event.getOrElse(UserEvent(id = -9999, url = "test"))


    path_visited  =  if(path_visited.isEmpty){
      path_visited :+ event.url
    } else{
      List(event.url.toString)
    }
      state.visited  = state.visited ++ path_visited
      state

  }


  def updateAcrossEvents(user:Int,
                         events: Iterator[UserEvent],
                         oldState: GroupState[UserSession]):UserSession = {
    var state:UserSession = if (oldState.exists) {
      //println("State exists")
      oldState.get
    }
    else {
      //println("State does not exist")
      UserSession(user,List.empty[String])
    }

    for (event <- events) {
      state = updateUserStateWithEvent(user, state, event)
      oldState.update(state)
    }
    state
  }


  class SessionCassandraForeachWriter extends ForeachWriter[UserSession] {

    /*
      - on every batch, on every partition `partitionId`
        - on every "epoch" = chunk of data
          - call the open method; if false, skip this chunk
          - for each entry in this chunk, call the process method
          - call the close method either at the end of the chunk or with an error if it was thrown
     */

    val keyspace = "blog"
    val table = "session"
    val connector = CassandraConnector(sparkSession.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      //println("Open connection")
      true
    }

    override def process(sess: UserSession): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |insert into $keyspace.$table("id","visited")
             |values (${sess.id}, '${sess.visited.toArray.mkString("-")}')
           """.stripMargin)
      }
    }

    override def close(errorOrNull: Throwable): Unit = {

    }



  }




  def main(args: Array[String]): Unit = {

    sparkSession.sparkContext.setLogLevel("WARN")
    //    val UserEventEncoder = Encoders.product[UserEvent]
    //
    import sparkSession.implicits._
    //
    //
//    val userEventsStream = sparkSession.readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 12346)
//      .load()
//      .as[String]

    val KAFKA_TOPIC_NAME_CONS = "eventTopic"

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .load()
      //.as[String

    val userEventsStream = df.selectExpr("CAST(value AS STRING)").as[String]

//    trans_df_1
//    .writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//    .awaitTermination()



    val finishedUserSessionsStream:Dataset[UserSession] =
      userEventsStream
        .map(deserializeUserEvent)
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(
          updateAcrossEvents)

    finishedUserSessionsStream.writeStream
      .outputMode(OutputMode.Update)
      .option("checkpointLocation", "checkpoint")
      .foreach(new SessionCassandraForeachWriter)
      .start()
    .awaitTermination()








  }

  def deserializeUserEvent(json: String): UserEvent = {
    json.decodeEither[UserEvent] match {
      case Right(userEvent) => userEvent
      case Left(error) =>
        println(s"Failed to parse user event: $error")
        UserEvent.empty
    }
  }

}