package example

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import example.ConsecutiveSteamFlow.Payload
import example.ProductCatalog.LocalizedProduct
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ConsecutiveStreamAppSpec extends AnyFunSpec with Matchers {

  it("should emit the correct number of elements") {
    implicit val system: ActorSystem = ActorSystem("ConsecutiveStreamApp")

    val deProducts: List[LocalizedProduct] =
      List(2, 5, 8, 9, 10, 12).map(id =>
        LocalizedProduct(id, s"Deutsches Produkt mit id $id")
      )

    val maxId = deProducts.maxBy(_.id).id
    val minId = deProducts.minBy(_.id).id

    val deSource = Source(deProducts).via(
      ConsecutiveSteamFlow.toConsecutiveStream(minId, maxId)
    )

    val consecutiveCatalog: Seq[Payload[LocalizedProduct]] = Await.result(
      deSource
        .runWith(Sink.seq),
      5.seconds
    )
    Await.ready(system.terminate, 5.seconds)

    val expectedIds = 2.to(12)
    val actualIds = consecutiveCatalog.map(payload =>
      Payload.id[LocalizedProduct].getId(payload)
    )

    actualIds should contain theSameElementsInOrderAs expectedIds

  }
}
