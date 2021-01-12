package exp.jobs

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import exp.api.SparkTable
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

object SparkSqlStreamingWriterJob {
  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
//      .master("local[*]")
      .getOrCreate()

    val streamingWriter = new StreamingWriter(new SparkTable(spark, "hdfs://IP:PORT/data/", "parquet"))

    import actorSystem.executionContext

    streamingWriter.run().onComplete { x =>
      spark.stop()
      actorSystem.terminate()
      x match {
        case Failure(exception) => exception.printStackTrace()
        case Success(value)     =>
      }
    }
  }
}
