package exp.api

import csw.EventFactory
import csw.params.core.formats.ParamCodecs._
import csw.params.events.SystemEvent
import io.bullet.borer.Json

case class SystemEventRecord(
    exposureId: String,
    obsEventName: String,
    eventId: String,
    source: String,
    eventName: String,
    eventTime: String,
    seconds: Long,
    nanos: Long,
    paramSet: String
)

object SystemEventRecord {
  def generate(): SystemEventRecord = generate(0, "startEvent", EventFactory.generateEvent())

  def generate(exposureId: Long, obsEventName: String, systemEvent: SystemEvent): SystemEventRecord = {
    SystemEventRecord(
      exposureId.toString,
      obsEventName,
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
