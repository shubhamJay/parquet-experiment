package exp.jobs

import akka.Done
import akka.actor.typed.ActorSystem
import exp.api.{EventServiceMock, SparkTable}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class StreamingWriter(sparkTable: SparkTable)(implicit actorSystem: ActorSystem[_]) {
  def run(): Future[Done] = {
    import actorSystem.executionContext

    sparkTable.delete()

    EventServiceMock
      .eventStream()
      .groupedWithin(10000, 2.seconds)
      .mapAsync(1)(sparkTable.append)
      .run()
  }
}
