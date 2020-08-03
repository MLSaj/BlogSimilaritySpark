name := "BlogSimilaritySpark"

version := "0.1"

scalaVersion := "2.12.4"
val sparkVersion = "3.0.0"
val postgresVersion = "42.2.2"

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
  "mysql" % "mysql-connector-java" % "8.0.18"
)
