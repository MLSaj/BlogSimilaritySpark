
import java.util.regex.Matcher

import Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._


import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._



import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

import user.{UserEvent, UserSession,USession}

import scala.collection.{mutable, _}
import argonaut.Argonaut._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.collection.Map
/** An example of using a State object to keep persistent state information across a stream. 
 *  In this case, we'll keep track of clickstreams on sessions tied together by IP addresses.
 */
object Sessionizer {
  
  /** This "case class" lets us quickly define a complex type that contains a session length and a list of URL's visited,
   *  which makes up the session data state that we want to preserve across a given session. The "case class" automatically
   *  creates the constructor and accessors we want to use.
   */

  val sparkSess = SparkSession.builder().master("local[*]").appName("My App").getOrCreate()
  val sc = sparkSess.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
  val new_data = sparkSess.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
  //val ssc = new StreamingContext("local[*]", "Sessionizer", Seconds(1))





  case class SessionData(val sessionLength: Long, var clickstream:List[String],var size:Int);
  
  /** This function gets called as new data is streamed in, and maintains state across whatever key you define. In this case,
   *  it expects to get an IP address as the key, a String as a URL (wrapped in an Option to handle exceptions), and 
   *  maintains state defined by our SessionData class defined above. Its output is new key, value pairs of IP address
   *  and the updated SessionData that takes this new line into account.
   */
  def trackStateFunc(batchTime: Time, ip: String, url: Option[String], state: State[SessionData]): Option[(String, SessionData)] = {
    // Extract the previous state passed in (using getOrElse to handle exceptions)
    import sparkSess.implicits._
    //val new_data = sparkSess.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
    val new_data_2 = sparkSess.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
    val small_df = new_data_2.filter($"title" === "Blog1").limit(1)
    val size = small_df.select("size").limit(1).as[Int].collect()(0)
    val previousState = state.getOption.getOrElse(SessionData(0, List(),-7))
    
    // Create a new state that increments the session length by one, adds this URL to the clickstream, and clamps the clickstream 
    // list to 10 items
    val newState = SessionData(previousState.sessionLength + 1L, (previousState.clickstream :+ url.getOrElse("empty")).take(10),size)
    
    // Update our state with the new state.
    state.update(newState)
    
    // Return a new key/value result.
    Some((ip, newState))
  }

  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
//    import sparkSess.implicits._
//    val small_df = new_data.filter($"title" === "Blog1").limit(1)
//    val size = small_df.select("size").limit(1)
//    val case1 = SessionData(10,List("Hello"),size)
//    println(case1.size.as[Int].collect()(0))


    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // We'll define our state using our trackStateFunc function above, and also specify a
    // session timeout value of 30 minutes.
    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the (ip, url) we want from each log line
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
        (ip, url)
      } else {
        ("error", "error")
      }
    })

    // Now we will process this data through our StateSpec to update the stateful session data
    // Note that our incoming RDD contains key/value pairs of ip/URL, and that what our
    // trackStateFunc above expects as input.
    val requestsWithState = requests.mapWithState(stateSpec)

    // And we'll take a snapshot of the current state so we can look at it.
    val stateSnapshotStream = requestsWithState.stateSnapshots()

    // Process each RDD from each batch as it comes in
    stateSnapshotStream.foreachRDD((rdd, time) => {

      // We'll expose the state data as SparkSQL, but you could update some external DB
      // in the real world.

      val spark = SparkSession
         .builder()
         .appName("Sessionizer")
         .getOrCreate()

      import spark.implicits._

      // Slightly different syntax here from our earlier SparkSQL example. toDF can take a list
      // of column names, and if the number of columns matches what's in your RDD, it just works
      // without having to use an intermediate case class to define your records.
      // Our RDD contains key/value pairs of IP address to SessionData objects (the output from
      // trackStateFunc), so we first split it into 3 columns using map().
      val requestsDataFrame = rdd.map(x => (x._1, x._2.sessionLength, x._2.clickstream,x._2.size)).toDF("ip", "sessionLength", "clickstream","size")

      // Create a SQL table from this DataFrame
      requestsDataFrame.createOrReplaceTempView("sessionData")

      // Dump out the results - you can do any SQL you want here.
      val sessionsDataFrame =
        spark.sqlContext.sql("select * from sessionData")
      println(s"========= $time =========")
      sessionsDataFrame.show()

    })

    // Kick it off
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}



