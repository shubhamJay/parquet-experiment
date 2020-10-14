package exp.jobs

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.github.mjakubowski84.parquet4s.{ParquetStreams, ParquetWriter}
import exp.api.{Constants, EventServiceMock, SystemEventRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.DurationInt

object StreamingWriteJob {
  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    writeMode = ParquetFileWriter.Mode.CREATE,
    compressionCodecName = CompressionCodecName.SNAPPY,
    hadoopConf = new Configuration() // optional hadoopConf
  )

  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")

    EventServiceMock
      .eventStream()
      .via(
        ParquetStreams
          .viaParquet[SystemEventRecord](Constants.StreamingDir)
          .withMaxCount(writeOptions.rowGroupSize)
          .withMaxDuration(1.seconds)
          .withWriteOptions(writeOptions)
          .build()
      )
      .runForeach { eventRecord =>
        val count = eventRecord.eventId.toInt
        if (count % 1000 == 0) {
          println(s"Finished writing $count records *********************")
        }
      }
  }
}
