package exp.jobs

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import exp.api.SparkTable
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

object DeltaLakeStreamingWriterJob {
  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
//      .master("local[*]")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val streamingWriter = new StreamingWriter(new SparkTable(spark, "hdfs://IP:PORT/data/", "delta"))

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
