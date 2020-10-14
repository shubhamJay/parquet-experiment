package exp.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import csw.EventFactory

import scala.concurrent.duration.DurationInt

object EventServiceMock {
  def eventStream(): Source[SystemEventRecord, NotUsed] = {
    Source
      .repeat(())
      .throttle(10, 1.millis)
      .statefulMapConcat { () =>
        var count = 0
        _ =>
          count += 1
          List(SystemEventRecord.from(EventFactory.generateEvent(count)))
      }
      .take(100000)
  }

  def captureSnapshot(): Seq[SystemEventRecord] = {
    (1 to 2000).map(_ => SystemEventRecord.from(EventFactory.generateEvent()))
  }
}
