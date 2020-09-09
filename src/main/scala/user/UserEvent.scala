package user

import argonaut.Argonaut._
import argonaut.CodecJson


case class UserEvent(id: Int, url: String,timestamp:Long)
object UserEvent {
  implicit def codec: CodecJson[UserEvent] =
    casecodec3(UserEvent.apply, UserEvent.unapply)("id", "url","timestamp")

  lazy val empty = UserEvent(-1, "", 0)
}
