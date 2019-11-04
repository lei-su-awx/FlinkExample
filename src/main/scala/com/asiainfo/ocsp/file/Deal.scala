package com.asiainfo.ocsp.file

class Deal {

  var id: String = _
  var info1: Double = _
  var info2: String = _

  def this(id: String, info1: Double, info2: String) {
    this
    this.id = id
    this.info1 = info1
    this.info2 = info2
  }
}
