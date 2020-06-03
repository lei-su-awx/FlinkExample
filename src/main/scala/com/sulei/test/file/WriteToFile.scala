package com.sulei.test.file

import java.util.Properties

import com.sulei.test.cep.{Event, EventSchema}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

object WriteToFile {
  def main(args: Array[String]): Unit = {
    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.setParallelism(1)
    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val canceledOutput = new OutputTag[String]("canceled")
    val waitingOutput = new OutputTag[String]("waiting")
    val over50Output = new OutputTag[String]("over50")
    val below50Output = new OutputTag[String]("below50")

    val props = new Properties()
    props.setProperty("bootstrap.servers", "10.1.236.139:6667,10.1.236.143:6667,10.1.236.78:6667")
    val source = new FlinkKafkaConsumer010[Event]("sulei_in4", new EventSchema(","), props).assignTimestampsAndWatermarks(new WM())
    val inputStream: DataStream[Event] = see.addSource(source)
    val incStream = inputStream.map(event => {
      val info1 = event.info1
      event.info1 = info1 + 1
      event
    })

    val outputStream = incStream.process(new ProcessFunction[Event, String] {
      override def processElement(value: Event, ctx: ProcessFunction[Event, String]#Context, out: Collector[String]): Unit = {
        if (value.info2.equals("canceled")) {
          ctx.output(canceledOutput, value.toString)
        } else if (value.info2.equals("waiting")) {
          ctx.output(waitingOutput, value.toString)
        } else if (value.info2.equals("succeed")) {
          if (value.info1 >= 50) {
            ctx.output(over50Output, value.toString)
          } else {
            ctx.output(below50Output, value.toString)
          }
        } else {
          out.collect(value.toString)
        }
      }
    })

    outputStream.getSideOutput(canceledOutput).writeAsText("./canceled.txt", WriteMode.OVERWRITE)
    outputStream.getSideOutput(waitingOutput).writeAsText("./waiting.txt", WriteMode.OVERWRITE)
    outputStream.getSideOutput(over50Output).writeAsText("./over50.txt", WriteMode.OVERWRITE)
    outputStream.getSideOutput(below50Output).writeAsText("./below50.txt", WriteMode.OVERWRITE)

    outputStream.print()
    see.execute("WriteToFile")
  }

  class WM extends AssignerWithPeriodicWatermarks[Event] {

    val maxOutOfOrderness = 5000L
    var currentMaxTimestamp: Long = 0L

    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }

    override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
      val timestamp = element.id.toLong
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      timestamp
    }
  }

}
