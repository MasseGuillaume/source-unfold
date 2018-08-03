import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

import scala.collection.mutable
import scala.util.{Success, Failure, Try}

import akka.stream.SourceShape
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}

object Extensions {
  implicit class QueueExtensions[A](private val self: mutable.Queue[A]) extends AnyVal {
    def dequeueN(n: Int): List[A] = {
      val b = List.newBuilder[A]
      var i = 0
      while (i < n) {
        val e = self.dequeue
        b += e
        i += 1
      }
      b.result()
    }
  }
}

class UnfoldSource[S, E](seeds: List[S],
                         flow: Flow[S, E, NotUsed])(loop: E => List[S])(bufferSize: Int)(implicit ec: ExecutionContext) extends GraphStage[SourceShape[E]] {

  import Extensions.QueueExtensions
  val out: Outlet[E] = Outlet("UnfoldSource.out")
  override val shape: SourceShape[E] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {  
    val frontier = mutable.Queue[S]()
    val buffer = mutable.Queue[E]()
    var inFlight = 0

    def isBufferFull() = buffer.size >= bufferSize
    frontier ++= seeds
    var downstreamWaiting = false

    def fillBuffer(): Unit = {
      val batchSize = Math.min(bufferSize - buffer.size, frontier.size)
      val batch = frontier.dequeueN(batchSize)
      inFlight += batchSize

      val toProcess =
        Source(batch)
          .via(flow)
          .runWith(Sink.seq)(materializer)

      val callback = getAsyncCallback[Try[Seq[E]]]{
        case Failure(ex) => {
          fail(out, ex)
        }
        case Success(es) => {
          inFlight -= es.size
          es.foreach{ e =>
            buffer += e
            frontier ++= loop(e)
          }
          if (downstreamWaiting && buffer.nonEmpty) {
            val e = buffer.dequeue
            downstreamWaiting = false
            sendOne(e)
          }
          ()
        }
      }

      toProcess.onComplete(callback.invoke)
    }
    override def preStart(): Unit = fillBuffer()

    def sendOne(e: E): Unit = {
      push(out, e)
      if (inFlight == 0 && buffer.size == 0 && frontier.size == 0) {
        completeStage()
      }
    }

    def onPull(): Unit = {
      if (buffer.nonEmpty) {
        sendOne(buffer.dequeue)
      } else {
        downstreamWaiting = true
      }

      if (!isBufferFull) {
        fillBuffer()
      }
    }

    setHandler(out, this)
  }
}

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
  import system.dispatcher
  implicit val materializer = ActorMaterializer()

  def unfold[S, E](seeds: List[S], flow: Flow[S, E, NotUsed])(loop: E => List[S]): Source[E, NotUsed] = {
    Source.fromGraph(new UnfoldSource(seeds, flow)(loop)(bufferSize = 100))
  }

  def main(args: Array[String]): Unit = {

    val items = List.newBuilder[Item]
    val keep = Sink.foreach{(x: Item) =>
      items += x
    }

    Await.result(
      unfold(List(0), itemFlow)(_.kids).runWith(keep),
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