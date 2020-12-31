package exp.api

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID

import exp.data.ParamSetJson

case class SystemEventRecord(
    date: String,
    hour: Int,
    minute: Int,
    eventId: String,
    source: String,
    eventName: String,
    eventTime: String,
    seconds: Long,
    nanos: Long,
    paramSet: String
)

object SystemEventRecord {
  def generate(): SystemEventRecord = generate(UUID.randomUUID().toString)

  def generate(eventId: String): SystemEventRecord = {
    val instant = Instant.now()
    val time    = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
    SystemEventRecord(
      time.toLocalDate.toString,
      time.getHour,
      time.getMinute,
      eventId,
      "wfos.blue.filter",
      "filter wheel",
      instant.toString,
      instant.getEpochSecond,
      instant.getNano,
      ParamSetJson.jsonString
    )
  }
}
