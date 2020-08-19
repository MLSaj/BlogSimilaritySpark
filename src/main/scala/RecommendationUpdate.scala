
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, State, StateSpec}
import user.{USession, UserEvent}

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
import user.UserSession
import jnr.posix.POSIXHandler

//java.lang.NoClassDefFoundError: jnr/posix/POSIXHandler

import org.apache.spark.sql.types._
object RecommendationUpdate{




  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Recommendation")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  import sparkSession.implicits._
  val readSessions = sparkSession.sqlContext
    .read
    .cassandraFormat("session", "blog").load()//.as[UserSession2]


  def main(args: Array[String]): Unit = {

    readSessions.show()
  }


}
