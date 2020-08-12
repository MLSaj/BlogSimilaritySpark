package user
import scala.collection.Map
case class UserSession(visited: collection.Map[String, Boolean],
                       current_vector: Option[org.apache.spark.ml.linalg.Vector],
                       recommendations:Array[String])
