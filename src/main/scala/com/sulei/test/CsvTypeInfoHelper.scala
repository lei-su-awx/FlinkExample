package com.sulei.test

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CsvTypeInfoHelper {

  def getTypeInfo(fields: mutable.Map[String, String]): RowTypeInfo = {
    val fieldNames = ArrayBuffer[String]()
    val fieldTypes = ArrayBuffer[TypeInformation[_]]()

    //    for (elem <- fields) {
    //      fieldNames + elem._1
    //      elem._2 match {
    //        case _ => fieldTypes + Types.STRING.toString
    //      }
    //    }

    fields.foreach(field => {
      fieldNames += field._1
      field._2 match {
        case "int" => fieldTypes += Types.INT
        case "sql_timestamp" => fieldTypes += Types.SQL_TIMESTAMP
        case _ => fieldTypes += Types.STRING
      }
    })

    new RowTypeInfo(fieldTypes.toArray, fieldNames.toArray)
  }

}
