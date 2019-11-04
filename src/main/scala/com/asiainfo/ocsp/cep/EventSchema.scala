package com.asiainfo.ocsp.cep

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation


class EventSchema(val delimiter: String) extends DeserializationSchema[Event]{
  override def deserialize(message: Array[Byte]): Event = Event.fromString(new String(message), delimiter)

  override def isEndOfStream(nextElement: Event): Boolean = false

  override def getProducedType: TypeInformation[Event] = {
    TypeInformation.of(classOf[Event])
  }
}
