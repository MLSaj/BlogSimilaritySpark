import org.apache.spark.sql.SparkSession
import sun.nio.ch.FileDispatcherImpl

object KafkaStreamsApp extends App {

  // Step 0. Imports for Scala API for Kafka Streams
  import org.apache.kafka.streams.scala._
  import ImplicitConversions._
  import Serdes._
  import argonaut._
  import org.apache.spark.launcher.SparkLauncher


//  val sparkLauncher = new SparkLauncher
//  //Set Spark properties.only Basic ones are shown here.It will be overridden if properties are set in Main class.
//  sparkLauncher.setSparkHome("/usr/local/Cellar/apache-spark/3.0.0/libexec")
//    .setAppResource("/Users/sajeedbakht/Documents/BlogSimilaritySpark/out/artifacts/BlogSimilaritySpark_jar/BlogSimilaritySpark.jar")
//    .setMainClass("com.testing.RecommendationUpdate")
//    .setMaster("local[*]")
//    .addAppArgs("301")

  // Step 1. Describe Topology
  // Consume records from input topic and produce records to upper topic
  val builder = new StreamsBuilder

  val uppers = builder
    .stream[String, String]("eventTopic")
    .foreach((k:String, json:String) => {
      println(json)
      println(Parse.parse(json))
      val url:String = Parse.parseWith(json, _.field("url").flatMap(_.string).getOrElse("Error!"), msg => msg)
      val id:String = Parse.parseWith(json, _.field("id").flatMap(_.string).getOrElse("Error!"), msg => msg)

      println(url)
      println(id)
      println("\n")

      if(id != "Error!" && json != "hi") {
        val sparkLauncher = new SparkLauncher
        //Set Spark properties.only Basic ones are shown here.It will be overridden if properties are set in Main class.
        sparkLauncher.setSparkHome("/usr/local/Cellar/apache-spark/3.0.0/libexec")
          .setAppResource("/Users/sajeedbakht/Documents/BlogSimilaritySpark/out/artifacts/BlogSimilaritySpark_jar/BlogSimilaritySpark.jar")
          .setMainClass("com.testing.RecommendationUpdate")
          .setMaster("local[*]")
          .addAppArgs(id)
        sparkLauncher.startApplication()
      }
      else{
        println("caught ya")
      }


    })// Consume records as a stream
    //.mapValues(_.toUpperCase)        // Transform (map) the values

  // Produce records to "upper" topic
//  uppers.to("upper")
//
//  // Print out records to stdout for debugging purposes
//  import org.apache.kafka.streams.kstream.Printed
//  val sysout = Printed
//    .toSysOut[String, String]
//    .withLabel("stdout")
//  uppers.print(sysout)

  // Step 2. Build Topology
  val topology = builder.build

  // You can describe the topology and just finish
  println(topology.describe())

  // That finishes the "declaration" part of developing a Kafka Stream application
  // Nothing is executed at this time (no threads have started yet)

  // Step 3. Specify Configuration
  import java.util.Properties
  val props = new Properties()
  import org.apache.kafka.streams.StreamsConfig
  val appId = this.getClass.getSimpleName.replace("$", "")
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
  props.put(StreamsConfig.CLIENT_ID_CONFIG, appId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")

  // Step 4. Create Kafka Streams Client
  import org.apache.kafka.streams.KafkaStreams
  val ks = new KafkaStreams(topology, props)


  ks.start

}

