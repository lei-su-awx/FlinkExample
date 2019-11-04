package com.asiainfo.ocsp.classify;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @author sulei
 * @date 2019/10/31
 * @e-mail sulei5@asiainfo.com
 */

public class ClassifyByType {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");
        properties.setProperty("group.id", "classify");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer010<>("input", new SimpleStringSchema(), properties));

        DataStream<ClassifyObject> classifyStream = inputStream.map(value -> {
            String[] values = value.split(",");
            return new ClassifyObject(values[0], values[1], Double.valueOf(values[2]));
        });

        OutputTag<String> type1 = new OutputTag<>("type1");
        OutputTag<String> type2 = new OutputTag<>("type2");
        OutputTag<String> type3 = new OutputTag<>("type3");
        OutputTag<String> type4 = new OutputTag<>("type4");

        SingleOutputStreamOperator<String> outputStream = classifyStream.process(new ProcessFunction<ClassifyObject, String>() {
            @Override
            public void processElement(ClassifyObject value, Context ctx, Collector<String> out) {
                switch (value.getType().toLowerCase()) {
                    case "type1":
                        ctx.output(type1, value.toString());
                        break;
                    case "type2":
                        ctx.output(type2, value.toString());
                        break;
                    case "type3":
                        if (value.getVolume() >= 50) ctx.output(type3, value.toString());
                        else ctx.output(type4, value.toString());
                        break;
                    default:
                        out.collect(value.toString());
                }
            }
        });

        outputStream.getSideOutput(type1).writeAsText("./type1.txt", FileSystem.WriteMode.OVERWRITE);
        outputStream.getSideOutput(type2).writeAsText("./type2.txt", FileSystem.WriteMode.OVERWRITE);
        outputStream.getSideOutput(type3).writeAsText("./type3.txt", FileSystem.WriteMode.OVERWRITE);
        outputStream.getSideOutput(type4).writeAsText("./type4.txt", FileSystem.WriteMode.OVERWRITE);
        outputStream.writeAsText("./type.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("Classify info by type");
    }
}
