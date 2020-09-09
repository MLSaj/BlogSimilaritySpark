package com.testing
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import com.testing.RecommendationUpdate.sparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.{Map, mutable}

//java.lang.NoClassDefFoundError: jnr/posix/POSIXHandler
object RecommendationUpdate{





  val sparkSession = SparkSession.builder()
    //.master("local[*]")
    .appName("Recommendation")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  var readSessions:DataFrame = sparkSession.sqlContext
    .read
    .cassandraFormat("session", "blog").load()


  val makeVisitUdf = udf {
    (visits: String ) => {
      visits.split("-")

    }
  }

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

    //readSessions.show()
    //val id_val = args(0).toInt
    val id_val = args(0).toInt

    val time_stamp:Long = args(1).toLong
    //println(readSessions.show())
    import sparkSession.implicits._
    import org.apache.spark.sql._
    var length:Long = 0
    //print(okay === 1)
    println(length)

    var fresh = false
    var time_processed = 0;
    var check_length = false;

    while(!check_length || !fresh){
      //println("Waiting for it to show")
      //Thread.sleep(10000)
      readSessions = sparkSession.sqlContext.read.cassandraFormat("session", "blog").load()

      //readSessions.show()
      import sparkSession.implicits._
      var count = readSessions.filter($"id" === id_val)
      //import sparkSession.implicits._
      //time_processed = readSessions.select("timestamp").as[Long].collect()(0)

      length = count.count()

      check_length = length == 1
      if(check_length){
        var time_processed = count.select("timestamp").as[Long].collect()(0)
        //println("Found it")
        //println(time_processed)
        //println(time_stamp)
        if(time_processed >= time_stamp) {
          //println("It's fresh")
          fresh = true
        }
      }

    }

    //println("Length check", check_length)




    if(check_length && fresh) {
      import sparkSession.implicits._
      val user = readSessions.filter($"id" === id_val).withColumn(colName = "visit_list", makeVisitUdf(readSessions("visited"))).limit(1)

      val visited_list = user.select("visit_list").as[Array[String]].collect()(0)
      val new_data = sparkSession.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://000.000.000.000:27017/blog.cleanVectors")))
      //new_data.show()

      val visited_vector = sparkSession.sparkContext.broadcast(new_data.filter(new_data("title").isin(visited_list: _*)))

      val not_visited_pre = new_data.filter(!new_data("title").isin(visited_list: _*))

      val not_visited = not_visited_pre.withColumn("sparse", makeSparseMapUdf(new_data("size"),
        new_data("values"), new_data("indices")))


      var empty: org.apache.spark.ml.linalg.Vector = Vectors.sparse(28, Array(0), Array(0.0000001))
      if (visited_list.length == 0) {
        //println("Nothing here")
      }
      for (blogName <- visited_list.takeRight(2)) {


        val current_event = visited_vector.value.filter($"title" === blogName)
        //val values: Array[Double] = current_event.select("values").as[Array[Double]].toLocalIterator()


        val values: Array[Double] = current_event.select("values").as[Array[Double]].collect()(0)
        val indices: Array[Int] = current_event.select("indices").as[Array[Int]].collect()(0)
        val size: Int = current_event.select("size").as[Int].collect()(0)
        val current_vector = Vectors.sparse(size, indices, values)
        empty = add_two_sparse(current_vector, empty)
      }

      val sims = not_visited.withColumn(colName = "similarities", compare_sparse(empty)
      (not_visited("sparse")))

      //sims.select("title").orderBy(desc("similarities")).limit(2).show()


      val similarities: Array[String] = sims.select("title").orderBy(desc("similarities")).limit(2).map(r => r(0).asInstanceOf[String]).collect()

      //write this to mongoDB
      //kafka
      //val data = Seq((id_val.toString,similarities.toString))
      import scala.math.max
      //cassandra
      val data = Seq((id_val, similarities, max(time_stamp,time_processed)))


      val rdd = sparkSession.sparkContext.parallelize(data)
      //for cassandandra
      val df = rdd.toDF("id", "recommendations", "timestamp")

      //for kafka
      //val df = rdd.toDF("id","recommendations").toDF("key","value")
      //df.show()

      df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .option("keyspace", "blog")
        .option("table", "recommendations")
        .save()
    }
//    df.write
//      .format("kafka")
//      .option("kafka.bootstrap.servers","localhost:9092")
//      .option("topic","testTopic")
//      .save()

    //Thread.sleep(1000000)






  }


}
