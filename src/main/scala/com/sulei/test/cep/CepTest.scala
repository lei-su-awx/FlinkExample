package com.sulei.test.cep

import org.apache.flink.api.scala._
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object CepTest {

  def main(args: Array[String]): Unit = {
    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.setParallelism(1)
    val messages: DataStream[String] = see.socketTextStream("10.1.236.139", 11112)
    val events: DataStream[Event] = messages.map(msg => {
      val m = msg.split(",")
      new Event(m(0), m(1).toInt, m(2))
    })
    val pattern = Pattern.begin[Event]("start")
      .subtype(classOf[Event])
      .where(new IterativeCondition[Event] {
        override def filter(event: Event, context: IterativeCondition.Context[Event]): Boolean = {
          event.info1 > 98
        }
      })
      .followedBy("mid")
      .subtype(classOf[Event])
      .where(new IterativeCondition[Event] {
        override def filter(event: Event, context: IterativeCondition.Context[Event]): Boolean = {
          event.info1 < 3
        }
      })
      .within(Time.seconds(20))

    val patternStream: PatternStream[Event] = CEP.pattern(events.keyBy("id"), pattern)
    val result: DataStream[String] = patternStream.select(pat => {
      val first = pat("start").toList.head
      val mid = pat("mid").toList.head
      first.id + "   " + first.info1 + "    " + mid.info1
//      first.info2 + "     =====>    " + mid.info2

    })

    //    patternStream.flatSelect(
    //      new PatternFlatTimeoutFunction[Event, String] {
    //      override def timeout(map: util.Map[String, util.List[Event]], l: Long, collector: Collector[String]): Unit = {
    //        collector.collect(map.values().stream().flatMap(ele => ele.getClass).collect(Collector))
    //      }
    //    },
    //      new PatternFlatSelectFunction[Event, String] {
    //      override def flatSelect(map: util.Map[String, util.List[Event]], collector: Collector[String]): Unit = {
    //        collector.collect()
    //      }
    //    })

        result.writeAsText("./ceptest", WriteMode.OVERWRITE)
        see.execute
  }
}