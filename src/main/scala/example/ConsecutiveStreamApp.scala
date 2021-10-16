package example

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import example.ConsecutiveSteamFlow.Payload._
import example.ConsecutiveSteamFlow.{GetId, Payload}
import example.ProductCatalog.LocalizedProduct
import example.ProductCatalog.LocalizedProduct.toId

import scala.concurrent._
import scala.concurrent.duration._

object ProductCatalog {
  case class LocalizedProduct(id: Int, name: String)

  object LocalizedProduct {
    implicit val toId: GetId[LocalizedProduct] = (t: LocalizedProduct) => t.id
  }

  val deProducts: List[LocalizedProduct] = List(1, 2, 5, 8, 9, 10, 12).map(id =>
    LocalizedProduct(id, s"Deutsches Produkt mit id $id")
  )
  val enProducts: List[LocalizedProduct] = List(2, 4, 8, 9, 11).map(id =>
    LocalizedProduct(id, s"English Product with id $id")
  )

}

object ConsecutiveStreamApp extends App {

  implicit val system: ActorSystem = ActorSystem("ConsecutiveStreamApp")

  val maxId = Math.max(
    ProductCatalog.deProducts.maxBy(_.id).id,
    ProductCatalog.enProducts.maxBy(_.id).id
  )
  val minId = Math.min(
    ProductCatalog.deProducts.minBy(_.id).id,
    ProductCatalog.enProducts.minBy(_.id).id
  )

  val enSource = Source(ProductCatalog.enProducts).via(
    ConsecutiveSteamFlow.toConsecutiveStream(minId, maxId)
  )
  val deSource = Source(ProductCatalog.deProducts).via(
    ConsecutiveSteamFlow.toConsecutiveStream(minId, maxId)
  )

  val zippedSource = deSource
    .zip(enSource)

  val cleanedUpSource = zippedSource.filterNot {
    case (EmptyPayload(_), EmptyPayload(_)) => true
    case _                                  => false
  }

  val zipped = Await.result(
    zippedSource
      .runWith(Sink.seq),
    5.seconds
  )

  val cleaned = Await.result(
    cleanedUpSource
      .runWith(Sink.seq),
    5.seconds
  )

  def toMd(localizedProduct: LocalizedProduct): String = {
    s"| ${localizedProduct.id} | ${localizedProduct.name} |"
  }

  println("## deProducts")
  println("| id | name |")
  println("| -------: | ------- |")
  ProductCatalog.deProducts.foreach(p => println(toMd(p)))

  println("## enProducts")
  println("| id | name |")
  println("| -------: | ------- |")
  ProductCatalog.enProducts.foreach(p => println(toMd(p)))

  println("## zipped")
  displayZippedCatalog(zipped)

  println("## cleaned up (empty elements removed)")
  displayZippedCatalog(cleaned)

  def displayZippedCatalog(
      products: Seq[(Payload[LocalizedProduct], Payload[LocalizedProduct])]
  ) = {
    println("| de id | de product | en id| en product")
    println("| -------: | ------- | ------- | ------- |")

    def getName(payload: Payload[LocalizedProduct]) = {
      payload match {
        case WithPayload(LocalizedProduct(_, name)) => name
        case EmptyPayload(_)                        => "---"
      }
    }

    products.foreach { case (de, en) =>
      println(
        s"| ${id.getId(de)} | ${getName(de)} | ${id.getId(en)} | ${getName(en)} |"
      )

    }

  }

  Await.ready(system.terminate, 5.seconds)

}
