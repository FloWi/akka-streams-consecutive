package example

import akka.NotUsed
import akka.stream.scaladsl._
import example.ConsecutiveSteamFlow.Payload._

object ConsecutiveSteamFlow {

  trait GetId[T] {
    def getId(t: T): Int
  }

  sealed trait Payload[T]
  object Payload {
    case class WithPayload[T: GetId](msg: T) extends Payload[T]
    case class EmptyPayload[T](id: Int) extends Payload[T]

    implicit def id[T: GetId]: GetId[Payload[T]] = {
      case WithPayload(msg) => implicitly[GetId[T]].getId(msg)
      case EmptyPayload(id) => id
    }
  }

  def toConsecutiveStream[T](
      minId: Int,
      maxId: Int
  )(implicit id: GetId[T]): Flow[T, Payload[T], NotUsed] = {

    sealed trait StreamElement[+SE]
    object StreamElement {
      case object EndOfStream extends StreamElement[Nothing]
      case class Element[SE <: T](msg: SE) extends StreamElement[SE]
    }

    Flow[T]
      .map(StreamElement.Element.apply)
      .concat(
        Source.single(StreamElement.EndOfStream)
      ) //mark the end of the stream
      .statefulMapConcat { () =>
        var lastElement = Option.empty[T]

        { element =>
          (element, lastElement) match {
            case (StreamElement.Element(msg), None) =>
              // we haven't seen any elements.
              // We fill up the beginning of the stream with empty elements and emit them together
              // with the current element.
              // Capture the previous element for later

              val fillerElements =
                minId.until(id.getId(msg)).toList.map(EmptyPayload[T])
              lastElement = Some(msg)
              fillerElements ++ List(WithPayload(msg))

            case (StreamElement.Element(msg), Some(last)) =>
              // we have seen a previous element.
              // compare its id with the current id. If the diff is > 1, emit some empty elements to fill up the gap
              // Capture the previous element for later
              val diff = id.getId(msg) - id.getId(last)
              val missingIds: List[Int] = (id.getId(last) + 1)
                .until(id.getId(msg))
                .toList

              val fillerElements: List[Payload[T]] = if (diff > 1) {
                missingIds.map(i => EmptyPayload(i))
              } else List.empty

              lastElement = Some(msg)
              val msgsToEmit = fillerElements ::: List(WithPayload(msg))

              msgsToEmit

            case (StreamElement.EndOfStream, None) =>
              //looks like an empty stream
              List.empty

            case (StreamElement.EndOfStream, Some(last)) =>
              //we're at the end of the stream. We emit EmptyProducts until we reach the maxId
              val lastId = id.getId(last)
              if (lastId >= maxId)
                List.empty[Payload[T]]
              else
                lastId
                  .to(maxId)
                  .toList
                  .map(i => EmptyPayload(i))
          }
        }
      }
  }
}
