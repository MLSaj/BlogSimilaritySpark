package user
case class UserSession(visited: Map[String,Boolean],
                       current_vector: Option[org.apache.spark.ml.linalg.Vector],
                       recommendations:List[String])
