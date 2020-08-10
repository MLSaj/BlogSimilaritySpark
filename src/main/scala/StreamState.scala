import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import com.testing.SimilarityUpdate.makeMongoURI
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.{mutable, _}



object StreamState{

  val mongoURI = "mongodb://000.000.000.000:27017"

  val Conf = makeMongoURI(mongoURI,"blog","articles")
  val readConfigintegra: ReadConfig = ReadConfig(Map("uri" -> Conf))


  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/blog.articles")
    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/blog.vectors")
    .getOrCreate()



  // Uses the ReadConfig
  val df3 = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.articles")))

  def add_two_sparse(x:org.apache.spark.ml.linalg.Vector, y:org.apache.spark.ml.linalg.Vector)
  : org.apache.spark.ml.linalg.Vector= {

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

  //  def updateSessionEvents(
  //                           id: Int,
  //                           userEvents: UserEvent,
  //                           state: GroupState[UserSession]): Option[UserSession] = {
  //    if (state.hasTimedOut) {
  //      // We've timed out, lets extract the state and send it down the stream
  //      state.remove()
  //      state.getOption
  //    } else {
  //      /*
  //       New data has come in for the given user id. We'll look up the current state
  //       to see if we already have something stored. If not, we'll just take the current user events
  //       and update the state, otherwise will concatenate the user events we already have with the
  //       new incoming events.
  //       */
  //      val currentState = state.getOption
  //
  //      currentState match {
  //        case Some(state: UserSession) => {
  //          val new_visited = state.visited + (userEvents.url -> true)
  //          val current_vector = state.current_vector
  //
  //          //val added_vector = state.current_vector
  //
  //
  //        }
  //
  //
  //          val updatedUserSession =
  //            currentState.fold(UserSession(userEvents.toSeq))(currentUserSession =>
  //              UserSession(currentUserSession.userEvents ++ userEvents.toSeq))
  //          state.update(updatedUserSession)
  //
  //          if (updatedUserSession.userEvents.exists(_.isLast)) {
  //            /*
  //         If we've received a flag indicating this should be the last event batch, let's close
  //         the state and send the user session downstream.
  //         */
  //            val userSession = state.getOption
  //            state.remove()
  //            userSession
  //          } else {
  //            state.setTimeoutDuration("1 minute")
  //            state.getOption
  //            //None
  //          }
  //      }
  //    }
  //  }


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

  def compare(size:Int, indices:Array[Int], values:Array[Double]) = udf {
    (y:org.apache.spark.ml.linalg.Vector) => {
      val x = Vectors.sparse(size,indices,values)
      x.dot(y)

    }
  }



  def main(args: Array[String]): Unit = {
    //reads data from mongo and does some transformations
    //val data = read_mongo()
    val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
    //new_data.show(20,false)
    //val norm_column = data.select("id", "norm")
    val with_sparse = new_data.withColumn("sparse", makeSparseMapUdf(new_data("size"),
      new_data("values"),new_data("indices")))

    with_sparse.printSchema()

    //with_sparse.show(20,false)

    import sparkSession.implicits._


    val small_df = with_sparse.filter($"id" === 1)
    val values:Array[Double] = small_df.select("values").as[Array[Double]].collect()(0)
    val indices:Array[Int] =  small_df.select("indices").as[Array[Int]].collect()(0)
    val size:Int = small_df.select("size").as[Int].collect()(0)

    compare _

    with_sparse.withColumn("similarities",compare(size,indices,values)(with_sparse("sparse"))).show(20,false)

    //println(values,indices,size)
    //val indices:Array[Double] = with_sparse.select("indices").filter
    //val vector = with_sparse.select("sparse").filter($"id" === 1)//.as[SparseVector].collect()





    //val data = read_mongo()
    //insert_arrays(data)





    //sparse_data.show(20,false)
    //    data.write.option("vectors", "blogs").mongo()
    //sparkSession.sparkContext.parallelize(data).saveToMongoDB()
    //val writeConfig = WriteConfig(Map("collection" -> "vectors", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkSession)))

    //MongoSpark.save(data, writeConfig)

    //new_values.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))

    //    val someSchema = List(
    //      StructField("number", IntegerType, true),
    //      StructField("word", StringType, true),
    //      StructField("array", ArrayType(IntegerType),true)
    //    )
    //
    //    val someData = Seq(
    //      Row(8, "bat", Array(1,2,3)),
    //      Row(64, "mouse",Array(4,5,6)),
    //      Row(-27, "horse",Array(1,1,1))
    //    )
    //
    //    val someDF = sparkSession.createDataFrame(
    //      sparkSession.sparkContext.parallelize(someData),
    //      StructType(someSchema)
    //    )
    //
    //    someDF.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.testDF")))






  }

}

