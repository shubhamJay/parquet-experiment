package exp.jobs

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetStreams}
import exp.api.{Constants, SystemEventRecord}
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.DurationInt

object StreamingReadJob {
  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")
    ParquetStreams
      .fromParquet[SystemEventRecord](
        path = Constants.StreamingDir,
        options = ParquetReader.Options(hadoopConf = new Configuration())
      )
      .throttle(1, 10.millis)
      .runForeach(println)
  }
}
