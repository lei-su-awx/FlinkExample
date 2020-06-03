package com.sulei.test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala.StreamTableEnvironment

import scala.collection.mutable

object StreamMain {
  def main(args: Array[String]): Unit = {
    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(see)

    val sourceMap = new mutable.LinkedHashMap[String, String]()
    sourceMap.put("imsi", "string")
    sourceMap.put("lac", "int")
    sourceMap.put("cell", "string")

    val sinkMap = new mutable.LinkedHashMap[String, String]()
    sinkMap.put("imsi", "string")
    sinkMap.put("lac", "int")
    //    sinkMap.put("cell", "string")
    //    sinkMap.put("proctime", "sql_timestamp")
    //    sinkMap.put("eventtime", "sql_timestamp")

    tableEnv.connect(SourceHelper.getKafka("sulei_in4"))
      .withFormat(FormatHelper.getCsvFormat(sourceMap))
      .withSchema(SchemaHelper.getSourceSchema(sourceMap))
      .inAppendMode()
      .registerTableSource("in")

    val table = tableEnv.scan("in")
    val table1 = table.select("imsi, lac, 'sulei' as cell, eventtime")
      .window(Tumble.over("30.second").on("eventtime").as("fw"))
      .groupBy("fw, imsi")
      .select("imsi, sum(lac) as lac")

    tableEnv.connect(SinkHelper.getKafka("sulei_out1"))
      .withFormat(FormatHelper.getCsvFormat(sinkMap))
      .withSchema(SchemaHelper.getSinkSchema(sinkMap))
      .inAppendMode()
      .registerTableSink("out1")
    table1.insertInto("out1")

    //    tableEnv.connect(SinkHelper.getKafka("sulei_out1"))
    //      .withFormat(FormatHelper.getCsvFormat(sinkMap))
    //      .withSchema(SchemaHelper.getSinkSchema(sinkMap))
    //      .inAppendMode()
    //      .registerTableSink("out2")
    //    table2.insertInto("out2")

    //    tableEnv.connect(SinkHelper.getKafka("sulei_out1"))
    //      .withFormat(FormatHelper.getCsvFormat(sinkMap))
    //      .withSchema(SchemaHelper.getSinkSchema(sinkMap))
    //      .inAppendMode()
    //      .registerTableSink("out")
    //    table.insertInto("out")

    see.execute("suleiflink")


  }
}