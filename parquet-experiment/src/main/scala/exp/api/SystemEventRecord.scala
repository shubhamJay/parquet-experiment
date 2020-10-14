package exp.api

import csw.params.core.formats.ParamCodecs._
import csw.params.core.generics.Parameter
import csw.params.events.{EventName, SystemEvent}
import csw.prefix.models.Prefix
import io.bullet.borer.Json

case class SystemEventRecord(
    eventId: String,
    source: String,
    eventName: String,
    eventTime: String,
    seconds: Long,
    nanos: Long,
    paramSet: String
) {
  def toEvent: SystemEvent =
    SystemEvent(
      Prefix(source),
      EventName(eventName),
      Json.decode(paramSet.getBytes()).to[Set[Parameter[_]]].value
    )
}

object SystemEventRecord {
  def from(systemEvent: SystemEvent): SystemEventRecord =
    SystemEventRecord(
      systemEvent.eventId.id,
      systemEvent.source.toString(),
      systemEvent.eventName.name,
      systemEvent.eventTime.value.toString,
      systemEvent.eventTime.value.getEpochSecond,
      systemEvent.eventTime.value.getNano,
      Json.encode(systemEvent.paramSet).toUtf8String
    )
}
