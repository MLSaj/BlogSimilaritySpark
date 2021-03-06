name := "BlogSimilaritySpark"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.0"
val postgresVersion = "42.2.2"
val kafkaVersion = "2.4.0"


val cassandraConnectorVersion = "3.0.0-beta"
//val cassandraConnectorVersion = "2.5.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1",


  //"mysql" % "mysql-connector-java" % "5.1.12",
  "mysql" % "mysql-connector-java" % "8.0.18",

  //
// kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,


  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  ///"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.0.0",

  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,
  "joda-time" % "joda-time" % "2.3",

  "com.github.jnr" % "jnr-posix" % "2.0",
  //
  "io.argonaut" %% "argonaut" % "6.3.0",

  "org.scalaz" %% "scalaz-core" % "7.3.2"
)
