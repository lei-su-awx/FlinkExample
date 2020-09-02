package com.sulei.test.tablefactories;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSourceSinkFactory;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSourceSinkFactoryBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sulei
 * @date 2020/6/9
 * @e-mail 776531804@qq.com
 */

public class KafkaFactoryTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings es = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, es);

        KafkaTableSourceSinkFactoryBase kafkaTableSourceSinkFactoryBase = new KafkaTableSourceSinkFactory();
        StreamTableSource<Row> source = kafkaTableSourceSinkFactoryBase.createStreamTableSource(getKafkaSourceProperties());
        tableEnv.registerTableSource("input", source);

        Table table1 = tableEnv.scan("input");

        Table table2 = table1.select("imsi, cell, lac, proc");

        DataStream<Row> ds =  tableEnv.toAppendStream(table2, TypeInformation.of(Row.class));
        ds.print();

        env.execute("");
    }

    private static Map<String, String> getKafkaSourceProperties() {
        Map<String, String> properties = new HashMap<>(30);
        properties.put("schema.0.name", "imsi");
        properties.put("schema.0.type", "STRING");
        properties.put("schema.1.name", "lac");
        properties.put("schema.1.type", "STRING");
        properties.put("schema.2.name", "cell");
        properties.put("schema.2.type", "STRING");
        properties.put("schema.3.name", "proc");
        properties.put("schema.3.type", "SQL_TIMESTAMP");
        properties.put("schema.3.proctime", "true");

        properties.put("format.type", "csv");
        properties.put("format.schema", "ROW<imsi STRING, lac STRING, cell STRING>");
        properties.put("format.ignore-parse-errors", "true");
        properties.put("format.field-delimiter", ",");

        properties.put("connector.type", "kafka");
        properties.put("connector.topic", "output");
        properties.put("connector.startup-mode", "latest-offset");
        properties.put("connector.properties.0.key", "bootstrap.servers");
        properties.put("connector.properties.0.value", "10.1.236.65:6667");
        properties.put("group.id", "cep");
        return properties;
    }

    private static Map<String, String> getKafkaSinkProperties() {
        Map<String, String> properties = new HashMap<>(20);

        return properties;
    }
}
