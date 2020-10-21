package db

import akka.Done
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.Source
import exp.api.SystemEventRecord
import scalikejdbc._

import scala.concurrent.{Future, blocking}

class DbIO(implicit session: AutoSession, actorSystem: ActorSystem[_]) {
  import actorSystem.executionContext

  def setup(): Boolean = {
    sql"""
         |DROP TABLE IF EXISTS events;
         |
         |create table events
         |(
         |    exposureid   varchar(50),
         |    obseventname varchar(50),
         |    eventid      varchar(50),
         |    source       varchar(50),
         |    eventname    varchar(50),
         |    eventtime    varchar(50),
         |    seconds      bigint,
         |    nanos        bigint,
         |    paramset     text
         |);
         |
         |alter table events owner to mushtaqahmed;
         |
         |""".stripMargin.execute()()
  }

  def write(records: Seq[SystemEventRecord]): Future[Done] = {
    Source(records).mapAsync(8)(write).run()
  }

  def write(record: SystemEventRecord): Future[Int] = {
    import record._
    Future {
      blocking {
        sql"""
             |insert into events (
             |  exposureid,
             |  obseventname,
             |  eventid,
             |  source,
             |  eventname,
             |  eventtime,
             |  seconds,
             |  nanos,
             |  paramset
             |) values (
             |  $exposureId,
             |  $obsEventName,
             |  $eventId,
             |  $source,
             |  $eventName,
             |  $eventTime,
             |  $seconds,
             |  $nanos,
             |  $paramSet

             |)
       |""".stripMargin.update()()
      }
    }
  }

  def read(exposureId: String)(implicit session: AutoSession): List[String] = {
    val query = sql"select paramset from events where exposureid = $exposureId"
    query.map(_.string("paramset")).list()()
  }

}
