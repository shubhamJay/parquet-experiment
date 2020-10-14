package exp.jobs

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.github.mjakubowski84.parquet4s.ParquetWriter
import exp.api.{Constants, EventServiceMock}

import scala.concurrent.duration.DurationInt

object StreamingWriteJob2 {
  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")

    var batchId = 0

    EventServiceMock
      .eventStream()
      .groupedWithin(10000, 1.second)
      .runForeach { batch =>
        scala.concurrent.blocking {
          batchId += 1
          val bid      = "%010d".format(batchId)
          val fileName = s"${Constants.StreamingDir}/$bid.parquet"
          ParquetWriter.writeAndClose(fileName, batch)
          println(s"Finished writing batch: $fileName *********************")
        }
      }
  }
}
