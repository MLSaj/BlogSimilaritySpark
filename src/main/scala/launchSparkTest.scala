
import org.apache.spark.launcher.SparkLauncher

object launchSparkTest {
  val sparkLauncher = new SparkLauncher
  //Set Spark properties.only Basic ones are shown here.It will be overridden if properties are set in Main class.
  sparkLauncher.setSparkHome("/usr/local/Cellar/apache-spark/3.0.0/libexec")
    .setAppResource("/Users/sajeedbakht/Documents/BlogSimilaritySpark/out/artifacts/BlogSimilaritySpark_jar/BlogSimilaritySpark.jar")
    .setMainClass("com.testing.SimilarityUpdate")
    .setMaster("local[*]")


  def main(args: Array[String]): Unit = {

    //sparkLauncher.launch()
    sparkLauncher.startApplication()



  }

}
