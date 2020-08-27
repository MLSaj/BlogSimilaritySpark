package streamTests
import argonaut._

object ParseExample extends App {

  val json:String = """
    { "name" : "Toddler", "age" : 2, "greeting": "gurgle!" }
  """

  // Parse getting either error message or json
  val result: Either[String, Json] =
    Parse.parse(json)

  // Parse ignoring error messages
  val option: Option[Json] =
    Parse.parseOption(json)

  // Parse handling success and failure with functions
  val greeting1: String =
    Parse.parseWith(json, _.field("greeting").flatMap(_.string).getOrElse("Hello!"), msg => msg)

  // Parse handling success and providing a default for failure
  val greeting2: String =
    Parse.parseOr(json, _.field("greeting").flatMap(_.string)getOrElse("Hello!"), "Oi!")

  class MyType(val id: Int, val url:String)

  import argonaut._, Argonaut._


  implicit def MyTypeCodec: CodecJson[MyType] = codec2(
    (id: Int, url: String) => new MyType(id, url),
    (myType: MyType) => (myType.id, myType.url)
  )("id", "url")


  println(greeting1)
  val json_string:String  = """{"id":398,"url":"Blog4"}"""

  implicit def PersonDecodeJson: DecodeJson[Person] =
    DecodeJson(c => for {
      name <- (c --\ "name").as[String]
      age <- (c --\ "age").as[Int]
      childs <- (c --\ "childs").as[List[Person]]
    } yield Person(name, age, childs))

  case class Person(name: String, age: Int, childs: List[Person])

//  object Person {
//    implicit def PersonCodecJson: CodecJson[Person] =
//      casecodec3(Person.apply, Person.unapply)("name", "age", "childs")
//  }
  val input = """
    [{"name": "parent1", "age": 31, "childs": [{"name": "child1", "age": 2, "childs": []}]},
     {"name": "parent2", "age": 29, "childs": []}
    ]
    """


  case class Event(id: Int, url:String)
  implicit def EventDecodeJson: DecodeJson[Event] =
    DecodeJson(c => for {
      id <- (c --\ "id").as[Int]
      url <- (c --\ "url").as[String]

    } yield Event(id, url))



  val input_2 =  """{"id":398,"url":"Blog4"}"""

  val persons = input.decodeOption[List[Person]].getOrElse(Nil)
  println(persons)

  val event:Option[Event] = input_2.decodeOption[Event]
  println(event)
  event match {
    case Some(value) =>
  }


}
