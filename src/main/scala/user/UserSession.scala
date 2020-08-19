package user
import org.apache.spark.sql.Dataset

import scala.collection.Map
case class UserSession(var id: Int,
                        var visited: List[String]
                      )
