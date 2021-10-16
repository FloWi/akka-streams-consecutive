package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._

import scala.concurrent._
import scala.concurrent.duration._

object Hello extends App {

  implicit val system: ActorSystem = ActorSystem("QuickStart")

  sealed trait MyProduct
  case class LocalizedProduct(id: Int, name: String) extends MyProduct
  case class EmptyProduct(id: Int) extends MyProduct

  case class Meta[T](t: Option[T], idxDiff: Long)

  def toConsecutiveStream(
      minId: Int,
      maxId: Int
  ): Flow[LocalizedProduct, MyProduct, NotUsed] = {

    sealed trait StreamElement[+T]
    object StreamElement {
      case object EndOfStream extends StreamElement[Nothing]
      case class Element[T](msg: T) extends StreamElement[T]
    }

    Flow[LocalizedProduct]
      .map(StreamElement.Element.apply)
      .concat(
        Source.single(StreamElement.EndOfStream)
      ) //mark the end of the stream
      .statefulMapConcat { () =>
        var lastElement = Option.empty[LocalizedProduct]

        { element =>
          (element, lastElement) match {
            case (StreamElement.Element(msg), None) =>
              // we haven't seen any elements.
              // We fill up the beginning of the stream with empty elements and emit them together
              // with the current element.
              // Capture the previous element for later

              val fillerElements = minId.until(msg.id).toList.map(EmptyProduct)
              lastElement = Some(msg)
              fillerElements ++ List(msg)

            case (StreamElement.Element(msg), Some(last)) =>
              // we have seen a previous element.
              // compare its id with the current id. If the diff is > 1, emit some empty elements to fill up the gap
              // Capture the previous element for later
              val diff = msg.id - last.id
              val fillerElements: List[EmptyProduct] = if (diff > 1) {
                (last.id + 1).until(msg.id).toList.map(EmptyProduct.apply)
              } else List.empty

              lastElement = Some(msg)
              val msgsToEmit = fillerElements ::: List(msg)

              msgsToEmit

            case (StreamElement.EndOfStream, None) =>
              //looks like an empty stream
              List.empty

            case (StreamElement.EndOfStream, Some(last)) =>
              //we're at the end of the stream. We emit EmptyProducts until we reach the maxId
              (last.id + 1).to(maxId).toList.map(EmptyProduct)
          }
        }

      }
  }

  val deProducts = List(1, 2, 5, 8, 9, 10, 12).map(id =>
    LocalizedProduct(id, s"Deutsches Produkt mit id ${id}")
  )
  val enProducts = List(2, 4, 8, 9, 11).map(id =>
    LocalizedProduct(id, s"English Product with id ${id}")
  )

  val maxId = Math.max(deProducts.maxBy(_.id).id, enProducts.maxBy(_.id).id)
  val minId = Math.min(deProducts.minBy(_.id).id, enProducts.minBy(_.id).id)

  val enSource = Source(enProducts).via(toConsecutiveStream(minId, maxId))
  val deSource = Source(deProducts).via(toConsecutiveStream(minId, maxId))

  val res = Await.result(
    deSource
      .zip(enSource)
      .runWith(Sink.seq),
    5.seconds
  )
  res.foreach(println)

  Await.ready(system.terminate, 5.seconds)

}
