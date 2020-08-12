package user

case class UserVector(visited: Map[String, Boolean],
                       current_vector: Option[org.apache.spark.ml.linalg.Vector])
