import org.apache.spark.launcher.SparkLauncher
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.internals.KStreamTransformValues
import com.mongodb.spark.config.ReadConfig


object launchSparkEvents {
  val sparkLauncher = new SparkLauncher
  //Set Spark properties.only Basic ones are shown here.It will be overridden if properties are set in Main class.
  sparkLauncher.setSparkHome("/usr/local/Cellar/apache-spark/3.0.0/libexec")
    .setAppResource("/Users/sajeedbakht/Documents/BlogSimilaritySpark/out/artifacts/BlogSimilaritySpark_jar/BlogSimilaritySpark.jar")
    .setMainClass("com.testing.RecommendationUpdate")
    .setMaster("local[*]")
    .addAppArgs("1")

  val bootstrapServers = "localhost:9092,localhost:9093"
  val topicName = "eventTopic"


  def main(args: Array[String]): Unit = {
    import org.apache.kafka.common.serialization.Serdes
    import org.apache.kafka.streams.StreamsConfig
    val props = new java.util.Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsTest")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

    import org.apache.kafka.streams.StreamsBuilder
    import org.apache.kafka.streams.kstream.KStream
    val streamsBuilder = new StreamsBuilder
    val kStream = streamsBuilder.stream(topicName)
    kStream.foreach((k: Integer, v: String) => println("Got it ")//sparkLauncher.startApplication()
    )
    // Lauch spark application
    //    val sparkLauncher1 = sparkLauncher.startApplication()
    //
    //    //get jobId
    //    val jobAppId = sparkLauncher1.getAppId
    //
    //    //Get status of job launched.THis loop will continuely show statuses like RUNNING,SUBMITED etc.
    //    while (true && sparkLauncher1.getState().toString != "FINISHED" ) {
    //      println(sparkLauncher1.getState().toString)
    //
    //    }
    val topology = streamsBuilder.build
    val streams = new KafkaStreams(topology, props)
    streams.start()


    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      def foo() = {
        streams.close()
      }

      foo()
    }))


  }



}
