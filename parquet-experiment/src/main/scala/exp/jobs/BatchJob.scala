package exp.jobs

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}
import exp.api.{Constants, EventServiceMock, SystemEventRecord}

import scala.concurrent.blocking

object BatchJob {
  val obsEventIds = Seq(
    "obs-event-1",
    "obs-event-2",
    "obs-event-3"
  )

  def main(args: Array[String]): Unit = {
    obsEventIds.foreach { obsEventId =>
      println(s"writing snapshot captured for observe-event:$obsEventId ****************************")
      write(obsEventId, EventServiceMock.captureSnapshot())
    }

    obsEventIds.foreach { obsEventId =>
      println(s"reading snapshot captured for observe-event:$obsEventId ****************************")
      read(obsEventId)
    }
  }

  def write(batchId: String, batch: Seq[SystemEventRecord]): Unit =
    blocking {
      ParquetWriter.writeAndClose(s"${Constants.BatchingDir}/$batchId.parquet", batch)
    }

  def read(batchId: String): Unit = {
    val parquetIterable = ParquetReader.read[SystemEventRecord](s"${Constants.BatchingDir}/$batchId.parquet")
    try {
      parquetIterable.foreach(println)
    } finally parquetIterable.close()
  }

}
