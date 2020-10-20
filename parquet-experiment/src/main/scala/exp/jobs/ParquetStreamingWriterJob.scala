package exp.jobs

import java.io.File

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.github.mjakubowski84.parquet4s.{ParquetStreams, ParquetWriter}
import exp.api.{Constants, EventServiceMock, SystemEventRecord}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object ParquetStreamingWriterJob {
  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    writeMode = ParquetFileWriter.Mode.CREATE,
    compressionCodecName = CompressionCodecName.SNAPPY,
    hadoopConf = new Configuration() // optional hadoopConf
  )

  private val file = new File(Constants.StreamingDir)
  if (file.exists()) {
    FileUtils.deleteDirectory(file)
    println("deleted the existing table")
  }

  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")
    import actorSystem.executionContext

    EventServiceMock
      .eventStream()
      .via(
        ParquetStreams
          .viaParquet[SystemEventRecord](Constants.StreamingDir)
          .withMaxCount(writeOptions.rowGroupSize)
          .withMaxDuration(5.seconds)
          .withWriteOptions(writeOptions)
          .withPartitionBy("exposureId", "obsEventName")
          .build()
      )
      .statefulMapConcat { () =>
        var start = System.currentTimeMillis()

        eventRecord =>
          val count = eventRecord.eventId.toInt
          if (count % 10000 == 0) {
            val current = System.currentTimeMillis()
            println(s"Finished writing items: $count in ${current - start} milliseconds >>>>>>>>>>>>>>>>>>>>")
            start = current
          }
          List(eventRecord)
      }
      .run()
      .onComplete { x =>
        actorSystem.terminate()
        x match {
          case Failure(exception) => exception.printStackTrace()
          case Success(value)     =>
        }
      }
  }
}
