package com.sulei.test

import org.apache.flink.table.descriptors.{Csv, FormatDescriptor, Json}

import scala.collection.mutable

object FormatHelper {

  /**
    *
    * @param fields
    * @return
    */
  def getCsvFormat(fields: mutable.Map[String, String]): FormatDescriptor = {
    val typeInfo = CsvTypeInfoHelper.getTypeInfo(fields)
    val csvFormat = new Csv
    csvFormat.schema(typeInfo)
      .fieldDelimiter(',')
      .lineDelimiter("\r")
      .ignoreParseErrors()

    csvFormat
  }

  def getJsonFormat(): FormatDescriptor = {
    val jsonFormat = new Json
//    jsonFormat.jsonSchema()
    jsonFormat
  }
}
