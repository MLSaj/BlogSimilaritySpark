package user
import org.apache.spark.sql.Dataset

import scala.collection.Map
case class USession(var visited: collection.Map[String, Boolean],
                       var size : Int)
