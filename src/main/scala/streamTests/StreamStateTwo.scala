//package streamTests
//
//object StreamStateTwo {
//  import argonaut.Argonaut._
//  import com.mongodb.spark.config.ReadConfig
//  import com.mongodb.spark.sql._
//  import com.testing.SimilarityUpdate.makeMongoURI
//  import org.apache.spark.ml.linalg.Vectors
//  import org.apache.spark.sql.functions.udf
//  import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
//  import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
//  import user.{UserEvent, UserSession}
//
//  import scala.collection.{Map, mutable, _}
//
//
//
//    implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
//    implicit val userSessionEncoder: Encoder[Option[UserSession]] =
//      Encoders.kryo[Option[UserSession]]
//
//    val mongoURI = "mongodb://000.000.000.000:27017"
//
//    val Conf = makeMongoURI(mongoURI,"blog","articles")
//    val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))
//
//
//    val sparkSession = SparkSession.builder()
//      .master("local")
//      .appName("StreamStateTwo")
//      //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/blog.articles")
//      //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/blog.vectors")
//      .getOrCreate()
//
//
//    val makeSparseMapUdf = udf {
//      (size:Int, values:mutable.WrappedArray[Double], indices:mutable.WrappedArray[Int]) => {
//        //val vec = x.toSparse
//        //Vectors.sparse(size,indices,values)
//        //SparseVector(size,indices,values)
//        // size
//        //Vectors.dense(indices.toArray)
//        val new_indices = indices.toArray
//        //val new_values = Vectors.dense(values.toArray)
//        val new_values = values.toArray
//        val filter_values = new_values.filter(element => (element > 0.0 ))
//
//        Vectors.sparse(size,new_indices,values.toArray)
//
//
//      }
//    }
//
//
//    // Uses the ReadConfig
//    //val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))
//    val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
//    val with_sparse = new_data.withColumn("sparse", makeSparseMapUdf(new_data("size"),
//      new_data("values"),new_data("indices")))
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
//    def add_two_sparse(x:org.apache.spark.ml.linalg.Vector, y:org.apache.spark.ml.linalg.Vector)
//    : org.apache.spark.ml.linalg.Vector= {
//
//
//      //val y = option_y.getOrElse(Vectors.sparse(28,Array(0),Array(0.0001)))
//      val array_x = x.toArray
//      val array_y = y.toArray
//      val size = array_x.size
//      val add_array = (array_x, array_y).zipped.map(_ + _)
//      var i = -1;
//      val new_indices_pre = add_array.map( (element:Double) => {
//        i = i + 1
//        if(element > 0.0)
//          i
//        else{
//          -1
//        }
//      })
//
//      val new_indices:Array[Int] = new_indices_pre.filter(element => element != -1)
//
//      val final_add = add_array.filter(element => element > 0.0)
//      Vectors.sparse(size,new_indices,final_add)
//    }
//
//    def add_two_sparse_option(option_x:Option[org.apache.spark.ml.linalg.Vector], y:org.apache.spark.ml.linalg.Vector)
//    : org.apache.spark.ml.linalg.Vector= {
//
//      val x = option_x.getOrElse(Vectors.sparse(28,Array(0),Array(0.0001)))
//      val array_x = x.toArray
//      val array_y = y.toArray
//      val size = array_x.size
//      val add_array = (array_x, array_y).zipped.map(_ + _)
//      var i = -1;
//      val new_indices_pre = add_array.map( (element:Double) => {
//        i = i + 1
//        if(element > 0.0)
//          i
//        else{
//          -1
//        }
//      })
//
//      val new_indices:Array[Int] = new_indices_pre.filter(element => element != -1)
//
//      val final_add = add_array.filter(element => element > 0.0)
//      Vectors.sparse(size,new_indices,final_add)
//    }
//
//    def compare(size:Int, indices:Array[Int], values:Array[Double]) = udf {
//      (y:org.apache.spark.ml.linalg.Vector) => {
//        val x = Vectors.sparse(size,indices,values)
//        x.dot(y)
//
//      }
//    }
//
//
//    def compare_sparse(x:org.apache.spark.ml.linalg.Vector) = udf {
//      (y:org.apache.spark.ml.linalg.Vector) => {
//        x.dot(y)
//      }
//    }
//
//
//
//    def updateUserStateWithEvent(statez:UserSession, event:UserEvent):UserSession = {
//      println("Updating")
//
//      val empty_vector = Vectors.sparse(28, Array(3), Array(0.0001))
//      val empty_map = Map[String, Boolean]()
//      val empty_rec: Array[String] = Array("")
//      val empty_session = UserSession(empty_map, empty_vector, empty_rec)
//
//
//
//      val option_state = Option(statez)
//
//      val state = option_state.getOrElse(empty_session)
//
//      val past_vector = state.current_vector
//      var path_visited = state.visited
//      //val current_event = with_sparse.filter($"title" === event.url)
//
//      // val current_event = option_current_event.getOrElse(sparkSession.emptyDataFrame)
//      println("Showing current dataframe")
//
//      //current_event.show(3,false)
//
//      val option_event = Option(event)
//
//      val eventz = option_event.getOrElse(UserEvent(id = 0, url = "test"))
//
//
//
//
//      path_visited  =  if(path_visited.isEmpty){
//        path_visited + (eventz.url.toString -> true)
//      } else{
//        Map(eventz.url.toString -> true)
//      }
//
//      println("Paths length")
//      println(path_visited.size)
//      println("Current Event")
//      println(eventz.url)
//      print("States visits")
//      println(state.visited)
//
//      if(eventz.url != "test" &&  !(path_visited.isEmpty) ) {//&& !path_visited.exists(_._1 == eventz.url) && current_event.count() > 0 ){
//        println("Im in the if statements")
//        val current_vector = empty_vector
//
//        val total_vector = add_two_sparse(past_vector, current_vector)
//
//
//
//
//        state.current_vector = total_vector
//        state.visited  = state.visited ++ path_visited
//        state.recommendations = Array(event.url)
//        return state
//      }
//      else{
//        println("Im at the else statement")
//        state.visited  = state.visited ++ path_visited
//        state.recommendations = Array(event.url)
//        state
//      }
//    }
//
//
//
//
//    def updateAcrossEvents(user:Int,
//                           events: Iterator[UserEvent],
//                           oldState: GroupState[UserSession]):UserSession = {
//
//
//      var state:UserSession = if (oldState.exists) {
//        println("State exists with the following visited")
//        println(oldState.get.visited)
//        oldState.get
//      }
//      else {
//        println("State does not exist")
//        val empty_vector = Vectors.sparse(28, Array(3), Array(0.0001))
//
//        val empty_map = Map[String, Boolean]()
//
//        val empty_rec: Array[String] = Array("")
//        val empty_session = UserSession(empty_map, empty_vector, empty_rec)
//        empty_session
//      }
//      // we simply specify an old date that we can compare against and
//      // immediately update based on the values in our data
//      //val new_data:DataFrame = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
//      for (event <- events) {
//        //val compressed_data = new_data.filter($"title" === event.url)
//        state = updateUserStateWithEvent(state, event)
//        oldState.update(state)
//      }
//      state
//    }
//
//
//
//
//
//
//
//    def main(args: Array[String]): Unit = {
//
//      sparkSession.sparkContext.setLogLevel("WARN")
//
//      //    val UserEventEncoder = Encoders.product[UserEvent]
//      //
//      import sparkSession.implicits._
//
//
//      //
//      //
//      val userEventsStream = sparkSession.readStream
//        .format("socket")
//        .option("host", "localhost")
//        .option("port", 12345)
//        .load()
//        .as[String]
//
//
//
//
//
//
//      val finishedUserSessionsStream: Dataset[UserSession] =
//        userEventsStream
//          .map(deserializeUserEvent)
//          .groupByKey(_.id)
//          .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(
//            updateAcrossEvents)
//
//
//      finishedUserSessionsStream.writeStream
//        .outputMode(OutputMode.Update())
//        .format("console")
//        .option("checkpointLocation", "checkpoint")
//        .option("truncate",false)
//        .start()
//        .awaitTermination()
//
//
//
//
//
//
//    }
//
//    def deserializeUserEvent(json: String): UserEvent = {
//      json.decodeEither[UserEvent] match {
//        case Right(userEvent) => userEvent
//        case Left(error) =>
//          println(s"Failed to parse user event: $error")
//          UserEvent.empty
//      }
//    }
//
//
//}
