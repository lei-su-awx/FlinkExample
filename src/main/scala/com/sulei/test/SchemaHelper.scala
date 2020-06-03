package com.sulei.test

import org.apache.flink.table.descriptors.{Rowtime, Schema}
import org.apache.flink.api.scala.typeutils.Types

import scala.collection.mutable

object SchemaHelper {

  /**
    *
    * @param fields
    * @return
    */
  def getSourceSchema(fields: mutable.Map[String, String]): Schema = {
    val csvSchema = new Schema

    fields.foreach(field => {
      val fieldName = field._1
      val fieldType = field._2 match {
        case "string" => Types.STRING
        case "int" => Types.INT
        case "sql_timestamp" => Types.SQL_TIMESTAMP
        case _ => Types.STRING
      }
      csvSchema.field(fieldName, fieldType)
    })

    //    csvSchema.field("proctime", Types.SQL_TIMESTAMP)
    //      .proctime()

    val rowTime = new Rowtime
    rowTime.timestampsFromField("cell")
      .watermarksPeriodicBounded(30000)
    csvSchema.field("eventtime", Types.SQL_TIMESTAMP)
      .rowtime(rowTime)

    csvSchema
  }

  def getSinkSchema(fields: mutable.Map[String, String]): Schema = {
    val csvSchema = new Schema

    fields.foreach(field => {
      val fieldName = field._1
      val fieldType = field._2 match {
        case "string" => Types.STRING
        case "int" => Types.INT
        case "sql_timestamp" => Types.SQL_TIMESTAMP
        case _ => Types.STRING
      }
      csvSchema.field(fieldName, fieldType)
    })

    csvSchema
  }
}
