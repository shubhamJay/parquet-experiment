package csw

import csw.params.core.models.Id
import csw.params.events.{EventName, SystemEvent}
import csw.prefix.models.Prefix
import csw.time.core.models.UTCTime

object EventFactory {
  private val prefix    = Prefix("wfos.blue.filter")
  private val eventName = EventName("filter wheel")

  def generateEvent(): SystemEvent = SystemEvent(prefix, eventName, ParamSetData.paramSet)

  def generateEvent(id: Int): SystemEvent = SystemEvent(Id(id.toString), prefix, eventName, UTCTime.now(), ParamSetData.paramSet)
}
