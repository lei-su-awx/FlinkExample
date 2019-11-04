package com.asiainfo.ocsp

import scala.collection.mutable.ArrayBuffer

object TransHelper {

  val sinkComponents = ArrayBuffer[TableAndProperties]()
  val joinComponents = ArrayBuffer[TableAndProperties]()

  def handleTrans(sources: List[TableAndProperties]) = {

    sources.foreach(source => {
      val nextComponentIds = source.props("nextComponentIds").split(",")
      nextComponentIds.foreach {
        nextComponentId => {
          getComponentById(nextComponentId.trim)
          nextComponentId.trim match {
            case "column_select" => handleColumnSelect(source)
            case "row_select" => handleRowSelect(source)
            case "label" => handleLabel(source)
            case "join" => handleJoin(source)
            case "window" => handleWindow(source)
            case "agg" => handleAgg(source)
            case "event" => handleEvent(source)
            case "unknown" => handleUnknown(source)
          }
        }
      }
    })
  }

  def handleRowSelect(component: TableAndProperties) = {

  }

  def handleColumnSelect(component: TableAndProperties) = {}

  def handleLabel(component: TableAndProperties) = {}

  def handleJoin(component: TableAndProperties) = {
    joinComponents += component
  }

  def handleWindow(component: TableAndProperties) = {}

  def handleAgg(component: TableAndProperties) = {}

  def handleEvent(component: TableAndProperties) = {
    sinkComponents += component
  }

  def handleUnknown(component: TableAndProperties) = {}

  def getComponentById(id: String) = {}
}
