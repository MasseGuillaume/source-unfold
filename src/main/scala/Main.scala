import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {
  type ItemId = Int
  type Data = String
  case class Item(data: Data, kids: List[ItemId])

  // 0 => [1, 9]
  // 1 => [10, 19]
  // 2 => [20, 29]
  // ...
  // 9 => [90, 99]
  // _ => []
  // NB. I don't have access to this function, only the itemFlow.
  def nested(id: ItemId): List[ItemId] =
    if (id == 0) (1 to 9).toList
    else if (1 <= id && id <= 9) ((id * 10) to ((id + 1) * 10 - 1)).toList
    else Nil

  val itemFlow: Flow[ItemId, Item, NotUsed] = 
    Flow.fromFunction(id => Item(s"data-$id", nested(id)))


  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  def unfold[S, E](seed: S, flow: Flow[S, E, NotUsed])(loop: E => List[S]): Source[E, NotUsed] = {
    // todo: recurse
    Source.single(seed).via(flow)
  }

  def main(args: Array[String]): Unit = {

    val items = List.newBuilder[Item]
    val keep = Sink.foreach{(x: Item) =>
      println(x)
      items += x
    }

    Await.result(
      unfold(0, itemFlow)(_.kids).runWith(keep),
      Duration.Inf
    )

    val obtained = items.result()
    val expected = (0 to 99).map(x => Item(s"data-$x", nested(x)))

    if (obtained != expected) {
      println(obtained)
      println(expected)
    }

    system.terminate()
  }
}