package com.asiainfo.ocsp.cep;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author sulei
 * @date 2019/9/17
 * @e-mail 776531804@qq.com
 */

public class CepMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "xxxx:6667");
        properties.setProperty("group.id", "cep");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer010<>("input", new SimpleStringSchema(), properties));
        DataStream<CepObject> cepObjectStream = inputStream.map(value -> {
            String[] values = value.split(",");
            return new CepObject(values[0], values[1], Double.valueOf(values[2]), Long.valueOf(values[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        Pattern<CepObject, ?> pattern = Pattern.<CepObject>begin("start").where(new SimpleCondition<CepObject>() {
            @Override
            public boolean filter(CepObject value) {
                return value.getType().equalsIgnoreCase("succeed") && value.getVolume() < 3;
            }
        }).followedBy("end").where(new SimpleCondition<CepObject>() {
            @Override
            public boolean filter(CepObject value) {
                return value.getType().equalsIgnoreCase("succeed") && value.getVolume() > 100;
            }
        }).within(Time.seconds(20));

        PatternStream<CepObject> patternStream = CEP.pattern(cepObjectStream.keyBy("id"), pattern);
        DataStream<String> warningStream = patternStream.select(
                (PatternSelectFunction<CepObject, String>) pattern1 -> {
                    CepObject start = pattern1.get("start").get(0);
                    CepObject end = pattern1.get("end").get(0);
                    return start.getId() + " made a warning in " + start.getTimestamp() + " -> " + start.getVolume()
                            + " and " + end.getTimestamp() + " -> " + end.getVolume();
                });
        warningStream.print();

        env.execute("Cep in Flink");
    }
}
