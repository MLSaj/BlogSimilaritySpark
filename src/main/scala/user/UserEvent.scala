package user

import argonaut.Argonaut._
import argonaut.CodecJson


case class UserEvent(id: Int, url: String)
object UserEvent {
  implicit def codec: CodecJson[UserEvent] =
    casecodec2(UserEvent.apply, UserEvent.unapply)("id", "url")

  lazy val empty = UserEvent(-1, "")
}
