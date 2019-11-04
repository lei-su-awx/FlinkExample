package com.asiainfo.ocsp

import java.util.Properties

import org.apache.flink.table.descriptors.{ConnectorDescriptor, Kafka}

object SinkHelper {

  def getKafka(topic: String): ConnectorDescriptor = {

    val properties = new Properties()
    properties.setProperty("group.id", "sulei")
    properties.setProperty("bootstrap.servers", "10.1.236.139:6667,10.1.236.143:6667,10.1.236.78:6667")

    val kafkaSink = new Kafka
    kafkaSink.version("0.10")
      .topic(topic)
      .startFromGroupOffsets()
      .properties(properties)

    kafkaSink
  }

}
