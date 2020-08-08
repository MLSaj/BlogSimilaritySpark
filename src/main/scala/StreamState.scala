import org.apache.spark.mllib.linalg.Vectors




//(28,[1,4,7,9,10,11,12,14,15,17,18,20,21,22,23,24,26,27],[2.0,2.0,1.0,1.0,1.0,1.0,2.0,2.0,3.0,1.0,1.0,3.0,2.0,1.0,3.0,1.0,1.0,2.0])

object StreamState {

  val mongo_db = {

  }

//    def add_vectors() ={
//      val indices: Array[Int] = Array(1,2,3,4,7,11,12,13,14,15,17,20,22,23,24,25)
//      val norms: Array[Double] = Array(0.13028398104008743,0.23648605632753023,0.7094581689825907,0.13028398104008743,0.23648605632753023,0.0,0.14218861229025295,0.3580566057240087,0.14218861229025295,0.13028398104008743,0.26056796208017485,0.0,0.14218861229025295,0.06514199052004371,0.13028398104008743,0.23648605632753023)
//      val num_int = 28
//      val vector: Vector = Vectors.sparse(num_int, indices, norms)
//
//      val choiced_array = vector.toArray
//
//      choiced_array.map(element => print(element + " "))
//
//
//
//      val indices_2:Array[Int] = Array(1,4,7,9,10,11,12,14,15,17,18,20,21,22,23,24,26,27)
//      val norms_2:Array[Double] = Array(2.0,2.0,1.0,1.0,1.0,1.0,2.0,2.0,3.0,1.0,1.0,3.0,2.0,1.0,3.0,1.0,1.0,2.0)
//      val num_int_2 = 28
//      val vector_2:Vector = Vectors.sparse(num_int,indices_2,norms_2)
//
//      val add: Array[Double] = (vector.toArray, vector_2.toArray).zipped.map(_ + _)
//
//      var i = -1;
//      val new_indices_pre = add.map( (element:Double) => {
//        i = i + 1
//        if(element > 0.0)
//          i
//        else{
//          -1
//        }
//      })
//
//      val new_indices:Array[Int] = new_indices_pre.filter(element => element != -1)
//
//
//
//
//
//      //val array = vector.toArray
//
//      //array.map(element => println(element))
//      val final_add = add.filter(element => element > 0.0)
//
//      add.map(element => print(element + " "))
//      println("Printing indices")
//      new_indices.map(element => print(element + " "))
//
//      Vectors.sparse(num_int,new_indices,final_add)
//
//    }

  def add_two_sparse(x:org.apache.spark.mllib.linalg.Vector, y:org.apache.spark.mllib.linalg.Vector)
  : org.apache.spark.mllib.linalg.Vector= {

    val array_x = x.toArray
    val array_y = y.toArray
    val size = array_x.size
    val add_array = (array_x, array_y).zipped.map(_ + _)
    var i = -1;
    val new_indices_pre = add_array.map( (element:Double) => {
      i = i + 1
      if(element > 0.0)
        i
      else{
        -1
      }
    })

    val new_indices:Array[Int] = new_indices_pre.filter(element => element != -1)

    val final_add = add_array.filter(element => element > 0.0)
    Vectors.sparse(size,new_indices,final_add)
  }

//  def updateSessionEvents(
//                           id: Int,
//                           userEvents: UserEvent,
//                           state: GroupState[UserSession]): Option[UserSession] = {
//    if (state.hasTimedOut) {
//      // We've timed out, lets extract the state and send it down the stream
//      state.remove()
//      state.getOption
//    } else {
//      /*
//       New data has come in for the given user id. We'll look up the current state
//       to see if we already have something stored. If not, we'll just take the current user events
//       and update the state, otherwise will concatenate the user events we already have with the
//       new incoming events.
//       */
//      val currentState = state.getOption
//
//      currentState match {
//        case Some(state: UserSession) => {
//          val new_visited = state.visited + (userEvents.url -> true)
//          val current_vector = state.current_vector
//
//          //val added_vector = state.current_vector
//
//
//        }
//
//
//          val updatedUserSession =
//            currentState.fold(UserSession(userEvents.toSeq))(currentUserSession =>
//              UserSession(currentUserSession.userEvents ++ userEvents.toSeq))
//          state.update(updatedUserSession)
//
//          if (updatedUserSession.userEvents.exists(_.isLast)) {
//            /*
//         If we've received a flag indicating this should be the last event batch, let's close
//         the state and send the user session downstream.
//         */
//            val userSession = state.getOption
//            state.remove()
//            userSession
//          } else {
//            state.setTimeoutDuration("1 minute")
//            state.getOption
//            //None
//          }
//      }
//    }
//  }

  def main(args: Array[String]): Unit = {
    //add_vectors()
  }
}
