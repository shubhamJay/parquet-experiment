package exp.api

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.duration.DurationInt

object EventServiceMock {
  def eventStream(): Source[SystemEventRecord, NotUsed] = {
    val eventIds  = Iterator.from(1)
    val exposures =
      Iterator.from(1).flatMap { exposureId =>
        Iterator("startEvent", "endEvent").flatMap { obsEventName =>
          List.fill(5000)((exposureId, obsEventName))
        }
      }

    Source
      .fromIterator(() => eventIds)
      .throttle(500, 100.millis)
      .map { eventId => SystemEventRecord.generate(eventId.toString) }
      .take(120000)
  }

  def captureSnapshot(): Seq[SystemEventRecord] = {
    (1 to 2000).map(_ => SystemEventRecord.generate())
  }
}
