package com.sulei.test.time;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author sulei
 * @date 2020/3/3
 * @e-mail 776531804@qq.com
 */

public class ProcessingTimeMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        see.setParallelism(1);
        EnvironmentSettings es = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(see, es);

        Properties properties = new Properties();
        properties.setProperty("group.id", "sulei");
        properties.setProperty("bootstrap.servers", "10.1.236.139:6667,10.1.236.143:6667,10.1.236.78:6667");

        Kafka inputKafka = new Kafka().properties(properties).version("universal").topic("sulei_in");
        Schema inputSchema = new Schema()
                .field("proctime", DataTypes.TIMESTAMP()).proctime()
                .field("stringg", DataTypes.STRING())
                .field("intt", DataTypes.INT())
                .field("floatt", DataTypes.FLOAT())
                .field("doublee", DataTypes.DOUBLE())
                .field("longg", DataTypes.BIGINT());
        FormatDescriptor csv = new Csv().ignoreParseErrors();

        ste.connect(inputKafka)
                .withFormat(csv)
                .withSchema(inputSchema)
                .createTemporaryTable("input");

        Table input = ste.from("input");
        GroupWindowedTable window = input.window(Tumble.over("30.second").on("proctime").as("tt"));
        WindowGroupedTable group = window.groupBy("stringg, tt");
        Table result = group.select("tt.start as start, stringg, intt.sum as s2, floatt.sum as s3, doublee.sum as s4, longg.sum as s5");

        ste.toAppendStream(result, TypeInformation.of(Row.class)).print();

        see.execute();
    }
}
