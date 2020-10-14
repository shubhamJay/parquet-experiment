package exp.api

import java.io.File

import com.github.mjakubowski84.parquet4s.ParquetWriter
import org.apache.commons.io.FileUtils

import scala.concurrent.{ExecutionContext, Future, blocking}

class ParquetTable(tablePath: String) {

  private val file = new File(tablePath)
  var batchId      = 0

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
        val start    = System.currentTimeMillis()
        batchId += 1
        val bid      = "%010d".format(batchId)
        val fileName = s"$tablePath/$bid.parquet"
        ParquetWriter.writeAndClose(fileName, batch)
        val current  = System.currentTimeMillis()
        println(s"Finished writing items: ${batch.length} in ${current - start} milliseconds *********************")
      }
    }
}
