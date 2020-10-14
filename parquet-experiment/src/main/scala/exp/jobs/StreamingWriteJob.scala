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

object StreamingWriteJob {
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
      .runForeach { eventRecord =>
        val count = eventRecord.eventId.toInt
        if (count % 1000 == 0) {
          println(s"Finished writing $count records *********************")
        }
      }
      .onComplete { x =>
        actorSystem.terminate()
        x match {
          case Failure(exception) => exception.printStackTrace()
          case Success(value)     =>
        }
      }
  }
}
