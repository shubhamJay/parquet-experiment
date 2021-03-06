package exp.api

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ExecutionContext, Future, blocking}

class SparkTable(spark: SparkSession, tablePath: String, format: String) {

  import spark.implicits._

  private val file     = new File(tablePath)
  private val fullPath = file.getCanonicalPath

  def delete(): Unit = {
    blocking {
      if (file.exists()) {
        FileUtils.deleteDirectory(file)
        println("deleted the existing table")
      }
    }
  }

  def append(batch: Seq[SystemEventRecord])(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      blocking {
        val start   = System.currentTimeMillis()
        batch.toDF().write.mode("append").partitionBy("date", "hour", "minute").format(format).save(tablePath)
        val current = System.currentTimeMillis()
        println(s"Finished writing items: ${batch.length} in ${current - start} milliseconds >>>>>>>>>>>>>>>>>>")
      }
    }
}
