package com.sulei.test.cep

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

object CepKafka {

  def main(args: Array[String]): Unit = {
    val props = readFromFile(".." + File.separator + "CepProperties.properties")
    props.setProperty("bootstrap.servers", "10.1.236.139:6667,10.1.236.143:6667,10.1.236.78:6667")
    val see = StreamExecutionEnvironment.getExecutionEnvironment 

    val source = new FlinkKafkaConsumer010[Event](props.getProperty("input.topic", "input"), new EventSchema(props.getOrDefault("field.delimiter", ",").toString), props)
    val inputStream: DataStream[Event] = see.addSource(source)

    val pattern = Pattern.begin[Event]("start")
      .subtype(classOf[Event])
      .where(new SimpleCondition[Event] {
        override def filter(event: Event): Boolean = {
          event.info1 > props.getProperty("spending.upper.limit", "98").toDouble
        }
      })
      .followedBy("mid")
      .subtype(classOf[Event])
      .where(new SimpleCondition[Event] {
        override def filter(event: Event): Boolean = {
          event.info1 < props.getProperty("spending.lower.limit", "3").toDouble
        }
      })
      .within(Time.seconds(props.getProperty("warning.interval.second", "98").toInt))

    val patternStream: PatternStream[Event] = CEP.pattern(inputStream.keyBy("id"), pattern)

    val warnings = patternStream.select(pat => {
      val first = pat("start").toList.head
      val mid = pat("mid").toList.head
      s"WARNING! user ${first.id} has a illegal deal! "
      first.id + "   " + first.info1 + "    " + mid.info1
    })

    val sink = new FlinkKafkaProducer010[String](props.getProperty("bootstrap.servers", "10.1.236.139:6667,10.1.236.143:6667,10.1.236.78:6667"), props.getProperty("output.topic", "output"), new SimpleStringSchema())
    warnings.addSink(sink)

    see.execute(props.getProperty("application.name", "CepTest"))
  }

  def readFromFile(path: String): Properties = {
    val props = new Properties()
//    val jarPath = CepKafka.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
//    new File(jarPath)
    val jarPath = java.net.URLDecoder.decode(CepKafka.getClass.getProtectionDomain.getCodeSource.getLocation.getFile, "UTF-8")
    val propFile = new File(new File(jarPath + File.separator + path).getCanonicalPath)
    if (propFile.exists()) {
      val fileInputStream = new FileInputStream(propFile)
      props.load(fileInputStream)
      props
    } else {
      props
    }
  }
}
