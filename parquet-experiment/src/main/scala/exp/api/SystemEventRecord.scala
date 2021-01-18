package exp.api

import java.time.{Instant, LocalDateTime, ZoneOffset}

import csw.params.core.formats.ParamCodecs._
import csw.params.events.SystemEvent
import io.bullet.borer.Json

case class SystemEventRecord(
    date: String,
    hour: String,
    minute: String,
    eventId: String,
    source: String,
    eventName: String,
    eventTime: String,
    seconds: Long,
    nanos: Long,
    paramSet: String
)

object SystemEventRecord {
  def generate(systemEvent: SystemEvent): SystemEventRecord = {
    val instant = systemEvent.eventTime.value
    val time    = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
    SystemEventRecord(
      time.toLocalDate.toString,
      time.getHour.toString,
      time.getMinute.toString,
      systemEvent.eventId.id,
      systemEvent.source.toString(),
      systemEvent.eventName.name,
      systemEvent.eventTime.value.toString,
      systemEvent.eventTime.value.getEpochSecond,
      systemEvent.eventTime.value.getNano,
      Json.encode(systemEvent.paramSet).toUtf8String
    )
  }
}
