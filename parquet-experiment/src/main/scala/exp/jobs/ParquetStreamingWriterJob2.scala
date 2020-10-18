package exp.jobs

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import exp.api.{Constants, EventServiceMock, ParquetTable}

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object ParquetStreamingWriterJob2 {
  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")

    import actorSystem.executionContext

    val parquetTable = new ParquetTable(Constants.StreamingDir)
    parquetTable.delete()

    EventServiceMock
      .eventStream()
      .groupedWithin(10000, 2.second)
      .mapAsync(1)(parquetTable.append)
      .runForeach(_ => ())
      .onComplete { x =>
        actorSystem.terminate()
        x match {
          case Failure(exception) => exception.printStackTrace()
          case Success(value)     =>
        }
      }
  }
}
