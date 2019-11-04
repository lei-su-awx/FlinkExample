package com.asiainfo.ocsp.cep

class Event {

  var id: String = _
  var info1: Double = _
  var info2: String = _

  def this(id: String, info1: Double, info2: String) {
    this
    this.id = id
    this.info1 = info1
    this.info2 = info2
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case event: Event =>
        this.id == event.id
      case _ =>
        false
    }
  }

  override def toString = s"$id, $info1, $info2"
}

object Event {
  def fromString(msg: String, delimiter: String): Event = {
    if (msg.nonEmpty) {
      val contents = msg.split(delimiter)
      Event(contents(0), contents(1).toDouble, contents(2))
    } else {
      Event.apply
    }
  }

  def apply: Event = new Event()

  def apply(id: String, info1: Double, info2: String): Event = {
    new Event(id, info1, info2)
  }
}
